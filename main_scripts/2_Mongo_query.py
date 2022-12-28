# -*- coding: utf-8 -*-

from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
import psycopg2
import redis
from neo4j import GraphDatabase

import random
from faker import Faker
from datetime import datetime as DT
from datetime import timedelta
from random import randrange
import codecs
import os

#2 Генерация значений в MongoDB (Institite, Kafedra, Specialnost, Disciplines)
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


client = MongoClient('localhost',27017)

db = client.university
collection = db.institute

query_for_mongo = ("SELECT i.name, k.name, s.name, d.name, d.technical FROM public.institute i,public.kafedra k,public.specialnost s, public.disciplines d WHERE k.institute_id=i.id AND s.kafedra_id=k.id AND s.id = d.spec_id;")
cursor.execute(query_for_mongo)
res=cursor.fetchall()

for i in range(len(res)):
    db.institute.insert_one({
        'institute':[
            {
                'name':res[i][0],
                'cafedras':[
                    {
                        'name':res[i][1],
                        'specialnosts':[
                            {
                                'name': res[i][2],
                                'disciplines':[
                                    {
                                        'name':res[i][3],
                                        'technical':res[i][4]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    })

print("Mongo fill")