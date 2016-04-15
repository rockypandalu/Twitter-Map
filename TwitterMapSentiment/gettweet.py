#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
# import tweepy
import json
from HTMLParser import HTMLParser
# from pyes import *

#Variables that contains the user credentials to access Twitter API
access_token = '4920361883-GGE8CRImcW5yeaTHcPUMHhjLzxc92KpuDEndI2q'
access_token_secret = 'hwD2RixmQ7Rr4u0vQ6vjLBn8tW8ch4wB2BPV9rKC0fKcu'
consumer_key = 'nGhWE2iCT8LAXwlvBMoj2V9gy'
consumer_secret = 'vNOj57Y3USQ5FuGXkbeuqjhrh3fA0wzrrUB2ZQwSgiWRxeVQA5'


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        global queue
        a = json.loads(HTMLParser().unescape(data))
        if 'coordinates' in a:
            if a['coordinates']:
                if 'text' in a:
                    print "get"
                    response = queue.send_message(MessageBody=a['text'], MessageAttributes={
                        'Lat': {
                            'StringValue': str(a['coordinates']['coordinates'][1]),
                            'DataType': 'String'
                        },
                        'Lng': {
                            'StringValue': str(a['coordinates']['coordinates'][0]),
                            'DataType': 'String'
                        }
                    })

        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    # client = boto3.client('sqs')
    sqs = boto3.resource('sqs')
    # queue = sqs.create_queue(QueueName='test8', Attributes={'DelaySeconds': '0'})
    queue = sqs.get_queue_by_name(QueueName='test8')
    print(queue.url)
    # print(queue.attributes.get('DelaySeconds'))

    # conn.indices.create_index("test-index")
    # mapping={'longitude':{'store':'yes','type':'float'},'latitude':{'store':'yes','type':'float'},'message':{'store':'yes','type':'string'}}
    # conn.indices.put_mapping("test-type",{'properties':mapping}, ["test-index"])

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    GEOBOX_WORLD = [-180,-90,180,90]
    stream.filter(track=["Good"])
    stream.filter(locations=GEOBOX_WORLD)
    stream.filter(languages=["en"])

