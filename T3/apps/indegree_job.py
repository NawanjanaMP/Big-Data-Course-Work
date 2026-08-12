"""
Task 3 / Step 3.2 - PySpark distributed job over the SNAP web-BerkStan graph.

  (1) Lazily parse the raw text file into a DataFrame, stripping '#' metadata
      headers - nothing executes until an action is called.
  (2) Aggregate destination vertices to compute the in-degree distribution.
  (3) Rank and extract the Top 50 dominant destination nodes, with explicit
      memory optimisations: DataFrame CACHING (edges reused by three separate
      computations) and a BROADCAST JOIN (tiny top-50 table joined against the
      7.6M-row edge table without a shuffle).

Submit from inside the master container (Step 3.3):
  docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.executor.memory=1500m \
      /opt/apps/indegree_job.py --hold
"""

import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

DATA_PATH = "/opt/data/web-BerkStan.txt"
OUT_DIR = "/opt/data/output"


def main(hold: bool):
    spark = (
        SparkSession.builder
        .appName("BerkStan-InDegree-Analytics")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    # ---- (1) Lazy parse: transformations only, no job triggered yet --------
    raw = spark.read.text(DATA_PATH)                       # lazy
    edges = (
        raw.filter(~F.col("value").startswith("#"))        # strip metadata headers
           .select(F.split(F.col("value"), r"\s+").alias("p"))
           .select(
               F.col("p")[0].cast("long").alias("src"),
               F.col("p")[1].cast("long").alias("dst"),
           )
           .where(F.col("src").isNotNull() & F.col("dst").isNotNull())
    )

    # ---- Memory optimisation 1: cache the parsed edge list -----------------
    # Three separate computations below reuse `edges`; without caching Spark
    # would re-read and re-parse the text file for each one.
    edges.cache()
    n_edges = edges.count()                                # action -> materialises cache
    print(f"[metrics] parsed edges: {n_edges:,}  (+{time.time()-t0:.1f}s)")

    # ---- (2) In-degree per destination vertex ------------------------------
    in_deg = edges.groupBy("dst").agg(F.count("*").alias("in_degree"))

    # Structural distribution: how many nodes have in-degree k?
    degree_dist = (
        in_deg.groupBy("in_degree")
              .agg(F.count("*").alias("num_nodes"))
              .orderBy("in_degree")
    )
    print("[result] in-degree distribution (first 20 rows):")
    degree_dist.show(20)

    # ---- (3) Top 50 dominant destination nodes -----------------------------
    top50 = in_deg.orderBy(F.desc("in_degree")).limit(50)
    print("[result] Top 50 destination nodes by in-degree:")
    top50.show(50, truncate=False)

    # ---- Memory optimisation 2: broadcast join -----------------------------
    # Annotate every edge pointing at a top-50 node with that node's rank.
    # top50 is tiny (50 rows) -> broadcasting it to every executor avoids
    # shuffling the multi-million-row edge table across the network.
    from pyspark.sql.window import Window
    ranked = top50.withColumn(
        "rank", F.row_number().over(Window.orderBy(F.desc("in_degree")))
    )
    hub_edges = edges.join(broadcast(ranked), edges.dst == ranked.dst, "inner")
    share = hub_edges.count() / n_edges * 100
    print(f"[result] {share:.2f}% of all edges point at the top-50 hub nodes "
          f"(computed via broadcast join - inspect the SQL tab: "
          f"BroadcastHashJoin, no shuffle on the large side)")

    # ---- Persist results to the shared storage layer -----------------------
    degree_dist.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT_DIR}/in_degree_distribution")
    ranked.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT_DIR}/top50_destinations")

    print(f"[metrics] total wall-clock: {time.time()-t0:.1f}s")
    print(f"[metrics] output written to {OUT_DIR}/ on the shared volume")

    if hold:
        # Keep the driver (and the :4040 application UI) alive so Stage
        # durations, DAG, and Shuffle metrics can be captured for Step 3.3.
        input("\n>>> Application UI held open at http://localhost:4040 - "
              "take your screenshots, then press Enter to finish. ")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold", action="store_true",
                    help="pause at the end so the :4040 UI stays up for metric capture")
    args = ap.parse_args()
    main(args.hold)
