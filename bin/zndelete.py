#!/usr/bin/env python
import zc.zk
import sys
from optparse import OptionParser

def remove_node(server, dryrun, path):
	if not path[0] == '/':
		path = "/" + path
	conn = zc.zk.ZooKeeper(server)
	conn.delete_recursive(path, dryrun)

def option_parser():
	usage = '\n'.join(["usage: %prog [options] [path]", "  Removes a znode from zookeeper.  Will not removoe ephermal nodes or their parents."])
	parser = OptionParser(usage=usage)
	#parser.add_option("-c", "--config", dest="config", help="use the passed in knewton config")
	parser.add_option("-d", "--dry-run", dest="dryrun", action="store_true", default=False, help="Does a dry run that prints the nods that would be deleted.")
	parser.add_option("-s", "--server", dest="server", default="127.0.0.1:2181", help="Zookeeper server to connect to. defaults to '127.0.0.1:2181")
	return parser

def main():
	(options, args) = option_parser().parse_args()
	remove_node(options.server, options.dryrun, '/'.join(args))

if __name__ == "__main__":
	main()
