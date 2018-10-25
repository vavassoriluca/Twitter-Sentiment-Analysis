
from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from credentials import consumer_key, consumer_secret, access_token, access_token_secret

topic = b'twitter'
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages(topic, data.encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)
        keywords = "brexit gbrexit eu europe".split()


if __name__ == '__main__':
    print('producer stream')
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    while True:
        try:
            stream.sample()
            stream.filter(languages=["en"], track=keywords)
        except:
            pass