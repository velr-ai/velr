CREATE
  // People (multi-labels where it makes sense)
  (keanu:Person:Actor {name:'Keanu Reeves', born:1964, birthplace:'Beirut, Lebanon'}),
  (moss:Person:Actor {name:'Carrie-Anne Moss', born:1967, birthplace:'Burnaby, Canada'}),
  (fish:Person:Actor {name:'Laurence Fishburne', born:1961, birthplace:'Augusta, USA'}),
  (weaving:Person:Actor {name:'Hugo Weaving', born:1960, birthplace:'Ibadan, Nigeria'}),
  (nolan:Person:Director {name:'Christopher Nolan', born:1970, birthplace:'London, UK'}),
  (leo:Person:Actor {name:'Leonardo DiCaprio', born:1974, birthplace:'Los Angeles, USA'}),
  (jgl:Person:Actor {name:'Joseph Gordon-Levitt', born:1981, birthplace:'Los Angeles, USA'}),
  (elliot:Person:Actor {name:'Elliot Page', born:1987, birthplace:'Halifax, Canada'}),
  (hardy:Person:Actor {name:'Tom Hardy', born:1977, birthplace:'London, UK'}),
  (lana:Person:Director:Writer {name:'Lana Wachowski', born:1965}),
  (lilly:Person:Director:Writer {name:'Lilly Wachowski', born:1967}),

  // Movies (multi-label genres)
  (matrix:Movie:ScienceFiction:Action {
    title:'The Matrix', released:1999, imdb:'tt0133093', genres:['Sci-Fi','Action'], runtime:136
  }),
  (memento:Movie:Thriller {
    title:'Memento', released:2000, imdb:'tt0209144', genres:['Thriller','Mystery'], runtime:113
  }),
  (inception:Movie:ScienceFiction:Heist {
    title:'Inception', released:2010, imdb:'tt1375666', genres:['Sci-Fi','Heist'], runtime:148
  }),
  (tdkr:Movie:Action:Superhero {
    title:'The Dark Knight Rises', released:2012, imdb:'tt1345836', genres:['Action','Superhero'], runtime:164
  }),

  // Directed
  (lana)-[:DIRECTED]->(matrix),
  (lilly)-[:DIRECTED]->(matrix),
  (nolan)-[:DIRECTED]->(memento),
  (nolan)-[:DIRECTED]->(inception),
  (nolan)-[:DIRECTED]->(tdkr),

  // Acted in (edge properties = roles)
  (keanu)-[:ACTED_IN {roles:['Neo']}]->(matrix),
  (moss)-[:ACTED_IN  {roles:['Trinity']}]->(matrix),
  (fish)-[:ACTED_IN  {roles:['Morpheus']}]->(matrix),
  (weaving)-[:ACTED_IN {roles:['Agent Smith']}]->(matrix),

  (moss)-[:ACTED_IN  {roles:['Natalie']}]->(memento),

  (leo)-[:ACTED_IN   {roles:['Cobb']}]->(inception),
  (jgl)-[:ACTED_IN   {roles:['Arthur']}]->(inception),
  (elliot)-[:ACTED_IN {roles:['Ariadne']}]->(inception),
  (hardy)-[:ACTED_IN {roles:['Eames']}]->(inception),

  (hardy)-[:ACTED_IN {roles:['Bane']}]->(tdkr);
