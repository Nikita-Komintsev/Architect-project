from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase
import psycopg2
import redis

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


# Значения для запроса
student_limit = 10
phrase = 'Тема'
begin_date = '2022-09-01'
end_date = '2023-05-29'


query_body = {
    "query": {
        "query_string": {
            "query":  "*%s*" % (phrase),
			"default_field": "description"
        }
    }
}

#1
# id лекцции из материалов где фраза
materials_with_phrase = es.search(index='materials', body=query_body)
# print(materials_with_phrase)
materials_with_phrase_ids = []
for lection_id in materials_with_phrase['hits']['hits']:
	materials_with_phrase_ids.append(lection_id['_source']['lecture_id'])
print(materials_with_phrase_ids)

#2
students = []

for lection_id in materials_with_phrase_ids:
    for student in graphDB_Driver.session().run(
        "MATCH (l:Lecture{iid:$id})--(d:Disciplines)--(s:Specialnost)--(g:Group)--(st:Student) RETURN  st", id=str(lection_id)).data():
        students.append(student['st']['id_stud_code'])

#3
querry_pattern = '''SELECT (CAST(SUM(CASE WHEN visited THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) AS percent, student_id
FROM visits
WHERE student_id in (%s)
AND date_visit between '%s' AND '%s'
GROUP BY student_id
ORDER BY percent
LIMIT '%s'
'''
cursor.execute(querry_pattern % (','.join(map(str,students)), str(begin_date), str(end_date), str(student_limit)))
result=cursor.fetchall()

#4
print("\nОтчет:")
for i in result:
    print("№ "+str(i[1])+"\t"+redis.get(i[1]).decode()+"\t"+str(int(i[0]*100))+"%\t"+ "c "+ str(begin_date)+"\tпо "+str(end_date)+" - "+phrase)
