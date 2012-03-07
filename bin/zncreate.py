#!/usr/bin/env python
import zc.zk
import sys
from optparse import OptionParser

def create_node(server, text, path):
	if not path[0] == '/':
		path = "/" + path
	conn = zc.zk.ZooKeeper(server)
	conn.create_recursive(path, "", acl=zc.zk.OPEN_ACL_UNSAFE)
	conn.set(path, text)

def option_parser():
	usage = '\n'.join(["usage: %prog [options] [path]", "  Creates a znode on zookeeper."])
	parser = OptionParser(usage=usage)
	parser.add_option("-i", "--input", dest="input", action="store_true", default=False, help="Store input from stdin in the znode")
	#parser.add_option("-c", "--config", dest="config", help="use the passed in knewton config")
	parser.add_option("-s", "--server", dest="server", default="127.0.0.1:2181", help="Zookeeper server to connect to. defaults to '127.0.0.1:2181")
	return parser

def main():
	(options, args) = option_parser().parse_args()
	text = ""
	if options.input == True:
		text = '\n'.join(sys.stdin.readlines())
	create_node(options.server, text, '/'.join(args))

if __name__ == "__main__":
	main()
