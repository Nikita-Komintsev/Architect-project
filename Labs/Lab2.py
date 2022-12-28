from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase
import psycopg2
import redis
from pymongo import MongoClient

# config
# ES
index_name = "materials"
es = Elasticsearch("http://elastic:uw3yy=ACpH1pmh2EZrEK@localhost:9200")

# Neo
uri = "bolt://localhost:7687"
userName = "neo4j"
password = "root"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

#postgre
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

#Redis
redis = redis.Redis(host= 'localhost', port= '6379')

#Mongo
client = MongoClient('localhost',27017)

db = client.university
collection = db.institute

# Значения для запроса
semestr_for_query = 1 # 1 or 2
year_for_query = 2022 # 2022 or 2023

def get_dates(semestr,year):
    year = str(year)
    if semestr == 1:
            return (str(year + "-09-01"), str(year + "-12-29"))
    elif semestr == 2:
        return (str(year+ "-01-09"), str(year + "-05-31"))

detes_period = get_dates(semestr_for_query,year_for_query)


#1 id курсов из NEO с требованиями к тех.средствам
discip_names = []
discip_names =  graphDB_Driver.session().run("MATCH (d:Disciplines) where d.technical<>'' return d.name",yaer=str(year_for_query), semestr=str(semestr_for_query)).value()
print(discip_names)

lec_id=[]
with graphDB_Driver.session() as neo_session:
        lec_id.append(neo_session.run("MATCH (l:Lecture)--(tt:TimeTable) WHERE tt.lecture_id=l.iid and date($start)<date(tt.date)<date($end) RETURN  l.iid",start=str(detes_period[0]),end=str(detes_period[1])).data())

lec_ids=[]
for i in range(len(lec_id[0])):
        lec_ids.append((lec_id[0][i]['l.iid']))
print(lec_ids)

array = []

with graphDB_Driver.session() as neo_session:
    for disc in discip_names:
        count_st=(neo_session.run("MATCH (l:Lecture)--(d:Disciplines)--(s:Specialnost)--(g:Group)--(st:Student) WHERE  d.name=$name and l.iid in $lec  return count (distinct st)",name=disc,lec=(lec_ids)).data())
        count = (count_st[0]['count (distinct st)'])
        array.append((disc,count))
print(array)


print("\nОтчет:")
for line in array:
    cur=(collection.find({"institute.cafedras.specialnosts.disciplines.name": line[0]}))
    for document in cur:
        print("\nДисциплина:",document['institute'][0]['cafedras'][0]['specialnosts'][0]['disciplines'][0]['name'])
        print("Слушателей: "+str(line[1])+" за "+str(semestr_for_query)+" семестр "+str(year_for_query)+" года")
        print("Институт:",document['institute'][0]['name'])
        print("\tКафедра:",document['institute'][0]['cafedras'][0]['name'])
        print("\tСпециальность:",document['institute'][0]['cafedras'][0]['specialnosts'][0]['name'])
        print("\tТехническое оборудование:",document['institute'][0]['cafedras'][0]['specialnosts'][0]['disciplines'][0]['technical'])