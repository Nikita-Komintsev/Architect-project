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

#4 в ES таблица Materials
kolvo_materials = 100
get_array_lecture_id = "SELECT array_agg(id) FROM public.lecture"
cursor.execute(get_array_lecture_id)
array_lecture_ids=cursor.fetchall()

def get_lecture_id():
    l_id = random.randint(0,len(array_lecture_ids[0][0])-1)
    lecture_id = array_lecture_ids[0][0][l_id]
    return lecture_id

mappings = {
        "properties": {
            "description": {"type": "text"},
            "lecture_id": {"type": "integer"}
    }
}

index_name = "materials"

es = Elasticsearch("http://elastic:uw3yy=ACpH1pmh2EZrEK@localhost:9200")

try:
    es.indices.create(index="materials", mappings=mappings)
except:
    print("index materials exists")

docs = []
try:
    for i in range(kolvo_materials):
        docs.append({
            'description': 'Тема1.Введение Тема этих лекций – понять, как создавать безопасные системы, почему компьютерные системы иногда бывают небезопасными и как можно исправить положение, если что-то пошло не так. Не существует никакого учебника на эту тему, поэтому вы должны пользоваться записями этих лекций, которые также выложены на нашем сайте, и вы, ребята, должны их заблаговременно читать. Имеется также ряд вопросов, на которые вы должны будете ответить в письменной форме, а также вы можете прислать свои собственные вопросы до 10-00 часов вечера перед лекционным днём. И когда вы придёте на лекцию, мы обсудим ваши ответы и вопросы и выясним, что собой представляет эта система, какие проблемы решает, когда это работает и когда это не работает, и хороши ли эти способы в других случаях',
            'lecture_id' : get_lecture_id()
        })
    helpers.bulk(es, docs, index=index_name)
except Exception as err:
    print("Error in creating docs in ES (materials)!!! ", err)

print("ES fill")