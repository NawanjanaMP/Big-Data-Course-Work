// Task 4 / Step 4.3 - Graph querying & network analytics.
// Each query is prefixed with PROFILE so you can DOCUMENT EXECUTION BEHAVIOUR
// (db hits, operator tree, rows) as the brief requires. In the Browser the
// profile renders as a visual plan - screenshot it for each query.

// ---------------------------------------------------------------------------
// Q0 (helper): find good target ids in YOUR subset before running Q1-Q3.
// Returns the 5 most-cited patents - use these as $target / endpoint values.
MATCH (p:Patent)<-[:CITES]-()
RETURN p.id AS patent, count(*) AS incoming
ORDER BY incoming DESC LIMIT 5;

// ---------------------------------------------------------------------------
// Q1 - Direct neighbour layers of a specific target reference item.
// Replace 3858241 with an id from Q0. One relationship hop in each direction:
// no join tables, no multi-join penalty - this IS the graph-native advantage.
PROFILE
MATCH (target:Patent {id: 3338819})
OPTIONAL MATCH (target)-[:CITES]->(cited:Patent)
OPTIONAL MATCH (target)<-[:CITES]-(citing:Patent)
RETURN target.id                    AS patent,
       collect(DISTINCT cited.id)  AS references_out,
       collect(DISTINCT citing.id) AS cited_by;

// ---------------------------------------------------------------------------
// Q2 - Degree centrality: top-tier patents by volume of INCOMING citation
// branches (in-degree). Pure aggregation over relationship traversals.
PROFILE
MATCH (p:Patent)<-[c:CITES]-()
RETURN p.id AS patent, count(c) AS in_degree
ORDER BY in_degree DESC
LIMIT 10;

// ---------------------------------------------------------------------------
// Q3 - Shortest path between two distant node points, mapping hidden
// reference linkages. Use two ids from different parts of the subset
// (e.g. one hub from Q0 and one low-degree node). The undirected pattern
// finds linkage chains regardless of citation direction; cap depth at 15.
PROFILE
MATCH (a:Patent {id: 3338819}), (b:Patent {id: 3717571})
MATCH path = shortestPath((a)-[:CITES*..15]-(b))
RETURN [n IN nodes(path) | n.id] AS linkage_chain,
       length(path)              AS hops;

// If Q3 returns nothing, the two patents sit in disconnected components of
// this small subset - pick another pair. Discovering (and reporting) that the
// first-5000-edge subgraph is sparsely connected is itself a legitimate
// analytical observation worth writing up.
