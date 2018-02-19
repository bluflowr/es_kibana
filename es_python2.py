from elasticsearch import Elasticsearch
from datetime import datetime
import json

es = Elasticsearch() #defaults to localhost:9200

doc = {
    'author': 'pyladiesatx',
    'text': 'Hope you\'re enjoying our talk!',
    'timestamp': datetime.now(),
}
print('Adding in one entry')
res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
print(res['result'])

print('Retrieving added entry')
res = es.get(index="test-index", doc_type='tweet', id=1)
print(res['_source'])

#Here we refresh the index, to be immediately searchable
es.indices.refresh(index="test-index")

# Search for all docs in the index
res = es.search(index="test-index", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
# Search for a certain author in the index
res = es.search(index="test-index", body={"query": {"match": {'author': 'pyladiesatx'}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
# ignore 404 and 400
es.indices.delete(index='test-index', ignore=[400, 404])
