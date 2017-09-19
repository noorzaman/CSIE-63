from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4jj"))
session = driver.session()

# create new movie and attach directors, actors and roles
# https://neo4j.com/developer/python/#neo4j-python-driver
## 
session.run("MATCH (keanu:Actor { name:'Keanu Reeves'}) "
"CREATE (wick:Movie { title:'John Wick', year:'2014-05-11'}) "
"CREATE (keanu)-[:ACTS_IN {role: 'Wick'}]->(wick) "
"CREATE (dafoe:Actor { name:'William Dafoe' }) "
"CREATE (dafoe)-[:ACTS_IN {role: 'Marcus'}]->(wick) "
"CREATE (nyquist:Actor { name:'Michael Nyquist' }) "
"CREATE (nyquist)-[:ACTS_IN {role: 'Viggo'}]->(wick) "
"CREATE (chad:Director { name:'Chad Stahelski' }) "
"CREATE (chad)-[:DIRECTS]->(wick) "
"CREATE (leitch:Director { name:'David Leitch' }) "
"CREATE (leitch)-[:DIRECTS]->(wick) ")

result = session.run("MATCH (m:Movie) WHERE m.title = {title} "
"RETURN m.title AS title, m.year AS year",
{"title": "John Wick"})

for record in result:
        print("%s %s" %(record["title"], record["year"]))

session.close()
