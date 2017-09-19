from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4jj"))
session = driver.session()
path = "/Users/swaite/stirling/CSIE-63/assignment-2/data/input/"



### Actors ###

result = session.run("""
MATCH (n:Actor) 
RETURN n.name as name;
""")

f = open(path + 'actors.csv','w')
f.write("name\n")
for record in result:
	f.write(record["name"] +"\n")
f.close()

### Directors ###

result = session.run("""
MATCH (d:Director) 
RETURN d.name as name;
""")

f = open(path + 'directors.csv','w')
f.write("name\n")
for record in result:
	f.write(record["name"] +"\n")
f.close()


### Movies ###
result = session.run("""
MATCH (m:Movie) 
RETURN m.title as title, m.year as year
""")

f = open(path +'movies.csv','w')
f.write("title,year\n")
for record in result:
	f.write(record["title"] + "," + str(record["year"]) + "\n")
f.close()


### Relationships - ACTS IN  ###
f = open(path + 'relationships-acts-in.csv','w')
f.write("name,role,year,title\n")
result = session.run("""
MATCH p=(a:Actor)-[r:ACTS_IN]->(m:Movie) 
RETURN a.name as name, r.role as role, m.year as year, m.title as title
""")
for record in result:
	f.write(record["name"] + "," + record["role"] + "," + str(record["year"]) + "," + record["title"] + "\n")
f.close()

### Relationships - DIRECTS  ###
f = open(path + 'directs.csv','w')
f.write("name,year,title\n")
result = session.run("""
MATCH p=(d:Director)-[r:DIRECTS]->(m:Movie) 
RETURN d.name as name, m.year as year, m.title as title
""")
for record in result:
	f.write(record["name"] + "," + str(record["year"]) + "," + record["title"] + "\n")
f.close()


session.close() 
