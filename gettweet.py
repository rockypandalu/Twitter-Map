#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from HTMLParser import HTMLParser
from pyes import *

#Variables that contains the user credentials to access Twitter API
access_token=''
access_token_secret=''
consumer_key=''
consumer_secret=''



#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        global conn
        a = json.loads(HTMLParser().unescape(data))
        if 'coordinates' in a:
            if a['coordinates']:
                if 'text' in a:
                    #loc= (a['coordinates']['coordinates'][1],a['coordinates']['coordinates'][0])
                    conn.index({'location': {"lat":a['coordinates']['coordinates'][1],"lon":a['coordinates']['coordinates'][0]},'message':a['text']},"test-index","test-type")
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    conn = ES('127.0.0.1:9200')
    try:
        conn.indices.delete_index("test-index")
    except:
        pass
    conn.indices.create_index("test-index")
    mapping={"location":{"type":"geo_point"},'message':{'store':'yes','type':'string'}}

    # mapping={'longitude':{'store':'yes','type':'float'},'latitude':{'store':'yes','type':'float'},'message':{'store':'yes','type':'string'}}
    conn.indices.put_mapping("test-type",{'properties':mapping}, ["test-index"])

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    GEOBOX_WORLD = [-180,-90,180,90]
    stream.filter(locations=GEOBOX_WORLD)



