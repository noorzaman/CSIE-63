from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

keanu = "Keanu Reeves"

# Find a list of actors playing in movies in which Keanu Reeves played.

results = session.run("match(a1:Actor)-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(a:Actor {name:\""+keanu+"\"}) return distinct a1.name as name, m.title as title")

print("==== List of actors playing in movies in which %s played ====" %keanu)
for record in results:
	print("%s (Movie: %s)" %(record["name"], record["title"]))

print("\n")

# Find directors of movies in which K. Reeves played

results = session.run("match(d:Director)-[:DIRECTS]->(m:Movie)<-[:ACTS_IN]-(a:Actor {name:\""+keanu+"\"}) return d.name as name, m.title as title ")

print("==== Directors of movies in which %s played ====" %keanu)
for record in results:
        print("%s (Movie: %s)" %(record["name"], record["title"]))

session.close()

