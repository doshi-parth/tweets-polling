import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

consumerKey ="dummy"
consumerSecret = "dummy"
token = "dummy"
secret = "dummy"

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer = lambda m : json.dumps(m).encode('ascii'))
class TweetStreamListener(StreamListener):

	def on_data(self, data):
		try:
			producer.send("tweets", data)
		except KeyError as key:
			print(key)
			pass
	def on_error(self, status):
		print(status)

if __name__ == '__main__':

	listener = TweetStreamListener()
	auth = OAuthHandler(consumerKey, consumerSecret)
	auth.set_access_token(token, secret)
	stream = Stream(auth, listener)
	stream.filter(track="america", locations = [-122.75,36.8,-121.75,37.8], languages = ['en'])