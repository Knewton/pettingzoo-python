import zc.zk
import zookeeper
import multiprocessing
import sys
import pettingzoo.utils

ITEM_PATH = "/item";
TOKEN_PATH = "/token";

class DistributedBag(object):
	def __init__(self, connection, path):
		self.connection = connection
		self.path = path
		self.connection.create_recursive(path + ITEM_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.connection.create_recursive(path + TOKEN_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.write_lock = multiprocessing.RLock()
		self.read_lock = multiprocessing.RLock()
		self.ids = set()
		self.add_callbacks = []
		self.delete_callbacks = []
		self.deletion_handlers = {}
		self.children = self.connection.children(path + TOKEN_PATH)
		self.max_token = max_counter(self.children)
		self.children(self._process_children_changed)

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
			item_id - of node to deleteid_to_item_path(path, item_id) (returned by addElement)
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

	def add_listeners(self, add_callback=None, remove_callback=None):
		"""
		Allows caller to add callbacks for item addition and item removal.
		Parameters:
			add_callback - (Optional) callback that will be called when an id is created.
			remove_callback - (Optional) callback that will be called when an id is created.
		Callbacks are expected to have the following interface:
		Parameters:
			dbag - This object
			affected_id - The id that is being added or deleted, as appropriate to the call
		"""
		with self.write_lock:
			if add_callback:
				self.add_callbacks.append(add_callback)
			if remove_callback:
				self.delete_callbacks.append(remove_callback)
			return self.get_items()

	def get_items(self):
		"""
		Returns a set of presently existant item ids.
		Returns:
			set of integers
		"""
		with self.read_lock:
			return self.ids.copy()

	def _on_delete_id(self, removed_id):
		"""
		Called internally when an item is deleted.  Not intended for external use.
		"""
		with self.write_lock:
			self.ids.remove(removed_id)
			for callback in self.delete_callbacks:
				callback(self, removed_id)

	def _on_new_id(self, new_id):
		"""
		Called intenally when an item is added.  Not intended for external use.
		"""
		path = id_to_item_path(self.path, new_id)
		exists = pettingzoo.utils.Exists(self.connection, path, [self._process_deleted])
		with self.write_lock:
			self.deletion_handlers[new_id] = exists
			self.ids.add(new_id)
			for callback in self.add_callbacks:
				callback(self, new_id)

	def _process_children_changed(self, children):
		"""
		Callback used for Children object.  Not intended for external use.
		"""
		new_max = max_counter(children)
		with self.write_lock:
			while self.max_token < new_max:
				self.max_token += 1
				self._on_new_id(self.max_token)

	def _process_deleted(self, node):
		"""
		Callback used for Exists object.  Not intended for external use.
		"""
		del_id = counter_value(node.path)
		self._on_delete_id(del_id)
		with self.write_lock:
			del self.deletion_handlers[del_id]
        

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

