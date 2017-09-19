import csv
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4jj"))
session = driver.session()
path = "file:///Users/swaite/stirling/CSIE-63/assignment-2/data/input/"

### CLEAR DATABASE ###
session.run(""" match(n) detach delete n """)

#### Actor ###
session.run("""
LOAD CSV WITH HEADERS FROM "file:///actors.csv" AS line
CREATE (a:Actor {name:line.name});
""")

#### Movies ###
session.run("""
LOAD CSV WITH HEADERS FROM 'file:///movies.csv' AS line
CREATE (m:Movie {title:line.title, year:line.year});
""")

### Directors ###
session.run("""
LOAD CSV WITH HEADERS FROM "file:///directors.csv" AS line
CREATE (d:Director {name:line.name});
""")

###  RELATIONSHIPS - ACTS_IN ###
session.run("""
LOAD CSV WITH HEADERS FROM "file:///relationships-acts-in.csv" AS line
MATCH (m:Movie) WHERE m.title = line.title 
MATCH (a:Actor) WHERE a.name = line.name
CREATE (a)-[:ACTS_IN {role:line.role}]->(m)
""")

###  RELATIONSHIPS - DIRECTS ###
session.run("""
LOAD CSV WITH HEADERS FROM "file:///directs.csv" AS line
MATCH (d:Director) WHERE d.name = line.name
MATCH (m:Movie) WHERE m.title = line.title and m.year=line.year
CREATE (d)-[:DIRECTS]->(m)
""")

session.close()
