from flask import Flask, jsonify
from flask import render_template
import os
from pyes import *
from flask import request
import json
# from pyes.queryset import generate_model

from wsgiref.simple_server import make_server

application = Flask(__name__)

#application.add_url_rule('/', 'index', index())

@application.route('/')
def index():
    # geolocations = getFromElasticSearch("a")
    global KEYWORD
    conn = ES(['172.31.52.18:9200'])
    results=conn.search(MatchAllQuery())
    send=""
    for result in results:
        #print result
        send+=str(result["location"]["lat"])+" "+str(result["location"]["lon"])+","
    KEYWORD=''
    return render_template('lalala.html',geolocations=send[:(len(send)-1)])

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


def getFromElasticSearch(keyword):
    conn = ES(['172.31.52.18:9200'])
    q=TermQuery("message",keyword)
    results=conn.search(query=q)
    send=""
    for result in results:
        send+=str(result["location"]["lat"])+" "+str(result["location"]["lon"])+","
    return send[:(len(send)-1)]

def getMessageByLocation(location):
    conn = ES(['172.31.52.18:9200'])
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
     KEYWORD = ''
     application.run(host='0.0.0.0')
     make_server("127.0.0.1", 5000, application).serve_forever()
