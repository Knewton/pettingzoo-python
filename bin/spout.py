#!/usr/bin/env python
import zc.zk
import sys
import re
import memcache
import time
import uuid
from optparse import OptionParser
from altnode.thrift_wrappers import * 
from datetime import datetime
from calendar import timegm

class KestrelClient(memcache.Client):
	pass

def utc_now_timestamp():
	""" Returns milliseconds since the epoch in UTC time """
	return timegm(datetime.utcnow().utctimetuple())

def connect_to_kestrel(server):
	return KestrelClient([server]) 

def encode_param_update():
	update = InternalParameterUpdate.from_fields(
			organ='spiky',
			space='update',
			row_key=uuid.uuid1(),
			column_key='stuff',
			timestamp=utc_now_timestamp(),
			value=1.0)
	return update 

def gen_message(kclient, qname):
	msg = encode_param_update().to_bytes()
	kclient.set(qname, msg, 10) 

def flood_de_queues(server, qname, n):
	kclient = connect_to_kestrel(server)
	for i in range(0, n):
		print "adding message\n"
		gen_message(kclient, qname)

def option_parser():
	usage = '\n'.join(['usage: %prog [options] [path]', ' Puts messages on a kestrel queue'])
	parser = OptionParser(usage=usage)
	parser.add_option("-c", "--config", dest="config", help="where to find kestrel config in zookeeper")
	parser.add_option("-n", "--num_messages", dest="nmes", type=int, help="number of messages to generate")
	parser.add_option("-q", "--queue_name", dest="qname", help="name of queue to publish updates to")
	parser.add_option("-s", "--server", dest="server",
			default="127.0.0.1:22133", help="Kestrel server to connect to.  defaults to '127.0.0.1:22133'")
	#parser.add_option("-z", "--zookeeper", dest="zookeeper", default="127.0.0.1:22133", help="Zookeeper server to connect to. defaults to '127.0.0.1:2181")
	return parser

def main():
	(options, args) = option_parser().parse_args()
	flood_de_queues(options.server, options.qname, options.nmes)

if __name__ == '__main__':
	main()
