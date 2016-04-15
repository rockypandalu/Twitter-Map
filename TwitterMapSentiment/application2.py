from flask import Flask, jsonify, Response
from flask import render_template
import logging
import os
from pyes import *
import gevent
from flask import request
import json
import redis
# from pyes.queryset import generate_model
##from flask.ext.socketio import SocketIO, emit

from wsgiref.simple_server import make_server

application = Flask(__name__)

NEW = 0
@application.route('/', methods = ['GET', 'POST', 'PUT'])
def index():
    global KEYWORD,NEW
    global conn
    try:
        js = json.loads(request.data)
        all_message = js['Message']
        message_dict = json.loads(all_message)
        conn.index({'location': {"lat":message_dict['lat'],"lon":message_dict['Lng']},'message':message_dict['message'],'sentiment':message_dict['sentiment']},"test-index","test-type")
	if message_dict!=None:
            stream()
    except:
        print "fail"
#	NEW = 0	
   # print message_dict['message']
    results=conn.search(MatchAllQuery())
    send=""
    for result in results:
       # print result
        send+=str(result["location"]["lat"])+" "+str(result["location"]["lon"])+" "+str(result["sentiment"])+","
    KEYWORD=''
    message_dict = {}
    return render_template('lalala.html',geolocations=send[:(len(send)-1)])

@application.route('/stream')
def stream():
#    if message_dict!=None:
#        print 'fa'
     return Response(event(), mimetype="text/event-stream")

@application.route('/<keyword>')
def show_keyword_result(keyword):
    # show the user profile for that user
    global KEYWORD
    if keyword!='favicon.ico':
        KEYWORD = keyword
    geolocations = getFromElasticSearch(keyword)
    print KEYWORD
    return render_template('lalala.html',geolocations=geolocations)

@application.route('/location')
def getlocation():
    print 'hehe'
    a = request.args.get('lat',10,type=float)
    b = request.args.get('lng',10,type=float)
    results = getMessageByLocation((a,b))
    return results

def event():
    print 'yield'
    yield 'data: '+'NEW'+'\n\n'
    return 

def getFromElasticSearch(keyword):
    global conn
    q=TermQuery("message",keyword)
    results=conn.search(query=q)
    send=""
    for result in results:
        send+=str(result["location"]["lat"])+" "+str(result["location"]["lon"])+" "+str(result["sentiment"])+","
    return send[:(len(send)-1)]

def getMessageByLocation(location):
    global conn
    latitute,longitute=location
    print KEYWORD
    if KEYWORD=='':
        print "noo"
        q=MatchAllQuery()
    else:
        q=TermQuery("message",KEYWORD)
    f=GeoDistanceFilter("location",{"lat":latitute,"lon":longitute},'100mi')
    f.optimize_bbox = None
    q = FilteredQuery(q,f)
    print q.serialize()

    results = conn.search(q)
    ret = []
    try:
        for result in results:
            ret.append(result["message"]+'   ')
            # print result["message"]

        print jsonify(result = ret)
        return jsonify(result = ret)
    except:
        print "No twitter Nearby"

if __name__ == '__main__':
     conn = ES(['172.31.52.18:9200'])
     try:
        conn.indices.delete_index("test-index")
     except:
        pass
     KEYWORD = ''
     conn.indices.create_index("test-index")
     mapping={"location":{"type":"geo_point"},'message':{'store':'yes','type':'string'},'sentiment':{'store':'yes','type':'string'}}
     # mapping={'longitude':{'store':'yes','type':'float'},'latitude':{'store':'yes','type':'float'},'message':{'store':'yes','type':'string'},'sentiment':{'store':'yes','type':'string'}}
     conn.indices.put_mapping("test-type",{'properties':mapping}, ["test-index"])
     application.run(host='0.0.0.0',threaded=True)
     make_server("127.0.0.1", 5000, application).serve_forever()
     
