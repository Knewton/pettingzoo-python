import zc.zk
import zookeeper
import threading
import sys
import traceback

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

