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

#3 В Redis (Steudents)
redis = redis.Redis(
     host= 'localhost',
     port= '6379')

query_for_redis = ("SELECT id_stud_code,fio FROM public.students;")
cursor.execute(query_for_redis)
result=cursor.fetchall()

for i in range(len(result)):
    redis.set(str(result[i][0]),str(result[i][1]))

print("Redis fill")