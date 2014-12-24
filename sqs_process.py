import os
import boto.sqs
from boto.sqs.message import Message
from ConfigParser import SafeConfigParser

## Read from configuration file
path = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'config.ini'
parser = SafeConfigParser()
parser.read(path)
access_key = parser.get('aws_connection', 'access_key')
secret_key = parser.get('aws_connection', 'secret_key')
region_name = parser.get('sqs_connection', 'region_name')

## Class to process data from AWS SQS 
class DecircSQS(object):
	
	def __init__(self):
		self.SQS_connect = boto.sqs.connect_to_region(region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
	
	def read_data(self, queue_name): 
		queue = self.SQS_connect.get_queue(queue_name)
		return queue.read(wait_time_seconds=20)		
	
	def write_data(self, queue_name, data):
		queue = self.SQS_connect.get_queue(queue_name)
		message = Message()
		message.set_body(data)
		queue.write(message)
		
	def delete_data(self, queue_name, message): 
		queue = self.SQS_connect.get_queue(queue_name)
		return queue.delete_message(message)
				
