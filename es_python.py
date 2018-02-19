from elasticsearch import Elasticsearch
es = Elasticsearch() #defaults to localhost:9200

# Basics for quickly creating and deleting an elasticsearch index

# ignore 400 cause by IndexAlreadyExistsException when creating an index
print('Creating Elastic Search Index')
response = es.indices.create(index='test-index', ignore=400)
print(response)
# ignore 404 and 400
print('Deleting Elastic Search Index')
response = es.indices.delete(index='test-index', ignore=[400, 404])
print(response)
