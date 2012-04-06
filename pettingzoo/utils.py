import zc.zk
import zookeeper
import threading
import sys
import traceback
import logging
import logging.config

def connect_to_zk(servers, logging=None):
	"""
	Function used to connect to zookeeper for pettingzoo.multiprocessing.
	Parameters:
		server - list of zookeeper servers to connect to in the form of a comma
			seperated list of addresses:port'localhost:2181,10.5.2.1:2181'
	Returns:
		zc.zk.ZooKeeper connection
	"""
	if logging:
		zookeeper.set_log_stream(logging)
	conn = zc.zk.ZooKeeper(servers)
	conn.watches.lock = threading.RLock()
	return conn

def get_server_list(config):
	if isinstance(config, list):
		return ','.join(["%s:%s" % (c['host'], c['port']) for c in config])
	elif config.has_key('server_list'):
		return ','.join(["%s:%s" % (c['host'], c['port']) for c in config['server_list']])
	elif config.has_key('zookeeper'):
		return ','.join(config['zookeeper']['server_list'])
	elif config.has_key('header'):
		return "%s:%s" % (config['host'], config['port'])

def counter_path(path, counter):
	"""
	Takes in a path and a counter and returns a zookeeper appropriate path.
	Parameters:
		path - The full path before the counter
		counter - An integer used as the counter
	Returns:
		a zookeeper path with a counter.
	"""
	return "%s%010d" % (path, counter)

def counter_value(path):
	"""
	Converts a zookeeper path with a counter into an integer of that counter
	Parameters:
		path - a zookeeper path
	Returns:
		the integer encoded in the last 10 characters.
	"""
	return int(path[-10:])

def max_counter(children):
	"""
	Loops through a children iterator and returns the maximum counter id.
	Parameters:
		children: an iteratable object containing strings with the zookeeper id standard
	Returns:
		Maximum id
	"""
	numbers = [-1]
	for child in children:
		numbers.append(counter_value(child))
	return max(numbers)

def configure_logger(config_file=None, **kwargs):
	if config_file:
		logging.config.fileConfig(config_file)
	else:
		root = logging.getLogger()
		if root.handlers:
			for handler in root.handlers:
				root.removeHandler(handler)
		if not kwargs.has_key('level'):
			kwargs['level'] = logging.DEBUG
		if not kwargs.has_key('stream'):
			kwargs['stream'] = sys.stderr
		if not kwargs.has_key('format'):
			kwargs['format'] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
		logging.basicConfig(**kwargs)

def get_logger():
	return logging.getLogger("pettingzoo")

def min_predecessor(children, position):
	''' 
	Loops through predecessors of a node and returns smallest predecessor id
	Parameters:
	children - iterable object containing strings with the zookeeper id 
	position - id of the node whose predecessor you're looking for
	Returns:
	id - int, smallest id that is less than the id provided
	'''
	#TODO check child node exists
	predecessor = -1
	for child in children:
		counter = counter_value(child)
		if  predecessor < counter < position:
			predecessor = counter
	return predecessor 
