// Task 4 / Step 4.2 - Bulk graph ingestion via LOAD CSV.
//
// Run with cypher-shell (recommended - handles IN TRANSACTIONS natively):
//   docker compose exec -T neo4j cypher-shell -u neo4j -p 'BigData2026!' \
//       < cypher/01_import.cypher
//
// Or paste into the Browser at http://localhost:7474 - if the Browser
// complains about IN TRANSACTIONS, prefix the LOAD CSV statement with :auto

// 1) Uniqueness constraint FIRST. This also creates the backing index that
//    makes every MERGE below an O(log n) lookup instead of a full node scan -
//    without it, importing even 5,000 edges degrades to quadratic behaviour.
CREATE CONSTRAINT patent_id IF NOT EXISTS
FOR (p:Patent) REQUIRE p.id IS UNIQUE;

// 2) Batched ingestion loop: each CSV row becomes two MERGEd Patent nodes
//    connected by an explicit directed CITES relationship.
LOAD CSV WITH HEADERS FROM 'file:///citations.csv' AS row
CALL {
  WITH row
  MERGE (citing:Patent {id: toInteger(row.citing)})
  MERGE (cited:Patent  {id: toInteger(row.cited)})
  MERGE (citing)-[:CITES]->(cited)
} IN TRANSACTIONS OF 1000 ROWS;

// 3) Verify the ingest (screenshot these counts for your report)
MATCH (p:Patent) RETURN count(p) AS patents;
MATCH ()-[c:CITES]->() RETURN count(c) AS citations;
