/* Create nodes */
CREATE (sven:Person {name: 'SVEN', age: 42})
CREATE (bob:Person {name: 'Bob'})
CREATE (j:Person {name: 'John'}), (b:Person {name: 'Jan'})
CREATE (matrix:Movie {title: 'The Matrix'})
CREATE (inception:Movie {title: 'Inception'})

// Relation creation
CREATE (sven)-[:ACTED_IN]->(matrix)
CREATE (sven)-[:ACTED_IN]->(inception)


CREATE (sven)-[:DIRECTED]->(matrix)
CREATE (bob)-[:DIRECTED]->(inception); 

