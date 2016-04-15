import boto3
import boto.sns
from alchemyapi import AlchemyAPI
import json
alchemyapi = AlchemyAPI()

def send_http(message):
    try:
        REGION = 'us-east-1'
        ARN = 'arn:aws:sns:us-east-1:765517032934:Test'
        conn = boto.sns.connect_to_region(REGION)
        conn.publish(topic = ARN, message=message)
    except:
        print "Send http Fail"

def get_emotion(queue):
    for message in queue.receive_messages(MessageAttributeNames=['Lat','Lng'],MaxNumberOfMessages=10):
        print message.message_attributes
        print message.body
        response = alchemyapi.sentiment("text",message.body)
        # Get the custom author message attribute if it was set
        if message.message_attributes is not None:
            Lat = message.message_attributes.get('Lat').get('StringValue')
            Lng = message.message_attributes.get('Lng').get('StringValue')
        if response.get("docSentiment"):
            msg = {"message":message.body,"lat":Lat,"Lng":Lng,"sentiment":response["docSentiment"]["type"]};
            msg = json.dumps(msg)
            send_http(msg)
        message.delete()

if __name__ == '__main__':
    sqs = boto3.resource('sqs')
    while True:
        try:
            queue = sqs.get_queue_by_name(QueueName='test8')
            get_emotion(queue)
        except KeyboardInterrupt:
            exit()
