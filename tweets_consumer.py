from kafka import KafkaConsumer
from elasticsearch import Elasticsearch 
import json

if __name__ == "__main__":
	consumer = KafkaConsumer('tweets', value_deserializer = lambda m: json.loads(m.decode("ascii")))
	es = Elasticsearch([{'host':'localhost', 'port':9200}])
	if es.ping():
		print("Yaay")
	settings = {
		"settings":{
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		'mappings':{
			"properties":{
				"created_at":{"type": "text"},
				"text":{"type":"text"},
				"user":{
					"type":"nested",
					"properties":{
						"id":{"type": "long"},
						"name":{"type":"text"},
						"screen_name":{"type":"text"}
						}
					}
				},
			}
		}
	
	print(consumer)
		# print(msg)
	#if not es.indices.exists('test-index'):
	index_name = 'tweets-test-1'
	if not es.indices.exists(index_name):
		try:
			es.indices.create(index = index_name, body = settings)
		except Exception as ex:
			print(str(ex))
			pass
	for msg in consumer:
	 	#print(msg.value)
		try:
			dict_data = json.loads(msg.value)
			json_data = {
				"created_at": dict_data['created_at'],
				"text": dict_data["text"],
				"user":{
					"id": dict_data["user"]["id"],
					"name": dict_data["user"]["name"],
					"screen_name": dict_data["user"]["screen_name"],
				}
			}
			print(json_data)
			es.index(index = index_name, body = json_data)
		except Exception as ex:
			print(str(ex))
			pass
