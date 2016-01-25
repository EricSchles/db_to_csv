from db_api.engine import Client
import sqlalchemy

#First we create a sqlalchemy connection
#using this we can connect to any type of database we like
db = sqlalchemy.create_engine("sqlite:///foo.db")

#then all we need to do is create our client
client = Client(db,"config_file.csv")
#We can craft our own sql statements:
client.get("SELECT * FROM hello",["hi"],"hello")
#parameters: sql statement, column names, table name
#OR
#We can get everything in the db
client.get_all()

