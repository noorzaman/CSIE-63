from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('mykeyspace')
session.execute("INSERT INTO person (person_id, first_name, last_name, city, cell_1, cell_2, cell_3) VALUES (4, 'Tony', 'Tiger', 'PHX', '111', '2222', '3333')")
session.execute("INSERT INTO person (person_id, first_name, last_name, city, cell_1, cell_2, cell_3) VALUES (5, 'John', 'Claxton', 'SFO', '444', '5555', '6666')")
session.execute("UPDATE person set city = 'NYC' where person_id=5")
result = session.execute("SELECT * FROM person WHERE person_id=5")
row = result[0]
print row
