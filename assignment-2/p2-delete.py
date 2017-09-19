from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4jj"))
session = driver.session()

# create new movie and attach directors, actors and roles
# https://neo4j.com/developer/python/#neo4j-python-driver

session.run("""MATCH (n { name: 'David Leitch'}) DETACH DELETE n""")
session.run("""MATCH (n { name: 'Chad Stahelski'}) DETACH DELETE n""")
session.run("""MATCH (n { name: 'Michael Nyquist' }) DETACH DELETE n""")
session.run("""MATCH (n { name: 'William Dafoe' }) DETACH DELETE n""")

session.run("""MATCH (n { title: 'John Wick'}) DETACH DELETE n""")

session.run("MATCH (n:Director) REMOVE n:Director")
session.run("MATCH p=()-[r:DIRECTS]->() DELETE r")

session.close()
