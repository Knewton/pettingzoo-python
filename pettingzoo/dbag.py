import zc.zk
import zookeeper

def connect_to_zk(servers):
	return zc.zk.ZooKeeper(servers)

ITEM_PATH = "/item";
TOKEN_PATH = "/token";

class DistributedBag(object):
	def __init__(self, connection, path):
		self.connection = connection
		self.path = path
		self.connection.create_recursive(path + ITEM_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.connection.create_recursive(path + TOKEN_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.max_token = max_counter(self.connection.children(path + TOKEN_PATH))

	def add(self, data, ephemeral=True):
		"""
		Inserts an element into the bag.
		Parameters:
			data      - value stored at element
			ephemeral - if true, element is automatically deleted when backing framework is closed
		Returns:
			element id (required for deletion)
		"""
		flags = zookeeper.SEQUENCE
		if ephemeral:
			flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE
		newpath = self.connection.create(self.path + ITEM_PATH + ITEM_PATH, data, zc.zk.OPEN_ACL_UNSAFE, flags)
		item_id = counter_value(newpath)
		self.connection.acreate(id_to_token_path(self.path, item_id), "", zc.zk.OPEN_ACL_UNSAFE)
		if item_id > 0:
			try:
				self.connection.adelete(id_to_token_path(self.path, item_id - 1))
			except zookeeper.NoNodeException:
				pass # If it doesn't exist, that's ok
		return item_id

	def remove(self, item_id):
		"""
		Remove an item from the bag.
		Parameters:
			item_id - of node to delete (returned by addElement)
		Returns:
			true if node was deleted, false if node did not exist
		"""
		try:
			self.connection.delete(id_to_item_path(self.path, item_id))
			return True
		except zookeeper.NoNodeException:
			return False

	def get(self, item_id):
		"""
		Returns the data payload of a specific item_id's znode
		Parameters:
			item_id - to retrieve
		Returns:
			data stored at id (or absent if invalid id)
		"""
		try:
			return self.connection.get(id_to_item_path(self.path, item_id))[0]
		except zookeeper.NoNodeException:
			return None

	def add_listener(self, bag_listener):
		pass

	def get_items(self):
		pass

	def on_delete_id(self):
		pass

	def on_new_id(self):
		pass

	def process(self, event):
		pass

def id_to_item_path(path, item_id):
	"""
	Returns the full znode path for a given item id. NOTE: Does not check validity of id!
	Parameters:
		item_id - for which to compute path
	Returns:
		znode path of node representing item
	"""
	return counter_path(path + ITEM_PATH + ITEM_PATH, item_id)

def id_to_token_path(path, item_id):
	"""
	Returns the full znode path for a token given item id. NOTE: Does not check validity of id!
	Parameters:
		item_id - for which to compute path
	Returns:
		znode path of node representing item
	"""
	return counter_path(path + TOKEN_PATH + TOKEN_PATH, item_id)

def counter_path(path, counter):
	return "%s%010d" % (path, counter)

def counter_value(path):
	return int(path[-10:])

def max_counter(children):
	numbers = [-1]
	for child in children:
		numbers.append(counter_value(child))
	numbers.sort()
	return numbers[-1]

