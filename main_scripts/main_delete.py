# Очистить все таблицы в БД
from pymongo import MongoClient
import psycopg2
import redis
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase


# Монго Удалить все документы из коллекции
client = MongoClient('localhost',27017)

db = client.university
collection = db.institute

try:
   collection.delete_many({})
   print("1) Mongo clear")
except Exception as err:
    print("Mongo error! ",err)


# Посгрес
database_name = "university_db"
user_name = "postgres"
password = "12345"
host_ip = "localhost"
host_port ="5432"

connection = psycopg2.connect(
            database = database_name,
            user = user_name,
            password = password,
            host = host_ip,
            port = host_port
)

connection.autocommit = True
cursor = connection.cursor()
try:
    delete_query = "DROP TABLE public.visits;"
    cursor.execute(delete_query)
    delete_query = "DROP TABLE public.students;"
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.\"timeTable\"; "
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.group; "
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.lecture;"
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.disciplines;"
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.specialnost; "
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.kafedra;"
    cursor.execute(delete_query)
    delete_query = "DROP TABLE  public.institute;"
    cursor.execute(delete_query)
    print('2) Postgres clear')
except Exception as err:
    print("Error in Postgres delete",err)

#Redis
redis = redis.Redis(
     host= 'localhost',
     port= '6379')

try:
    keys = redis.keys('*')
    redis.delete(*keys)
    print("3) Redis clear")
except Exception as err:
    print("Error in redis delete ", err)

# ES
es = Elasticsearch("http://elastic:uw3yy=ACpH1pmh2EZrEK@localhost:9200")

try:
    es.indices.delete(index='materials')
    print("4) ElasticSearch clear")
except Exception as err:
    print("Error in ES delete!!! ",err)

# Neo
uri = "bolt://localhost:7687"
userName = "neo4j"
password = "root"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

#Удалить всё
with graphDB_Driver.session() as neo_session:
        neo_session.run("MATCH (n) DETACH DELETE n")
        print("5) Neo4J clear")