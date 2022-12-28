from elasticsearch import Elasticsearch

es = Elasticsearch("http://elastic:uw3yy=ACpH1pmh2EZrEK@localhost:9200")

try:
    es.indices.delete(index='materials')
    print("docs delete")
    # es.delete(index="materials")
except:
    print("Error delete!!!")