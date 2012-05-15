import zc.zk
import zookeeper
import multiprocessing
import sys
import traceback
import pettingzoo.utils
from pettingzoo.deleted import Deleted

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
		self.max_token = pettingzoo.utils.max_counter(self.children)
		self._cleanup_tokens(self.children, self.max_token)
		self.children(self._process_children_changed)
		self._populate_ids()

	def _populate_ids(self):
		ichildren = self.connection.children(self.path + ITEM_PATH)
		for child in ichildren:
			self._on_new_id(pettingzoo.utils.counter_value(child))

	def _cleanup_tokens(self, children, max_token):
		for child in children:
			token_id = pettingzoo.utils.counter_value(child)
			if token_id < max_token:
				try:
					self.connection.adelete(id_to_token_path(self.path, token_id))
				except zookeeper.NoNodeException:
					pass # If it doesn't exist, that's ok

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
		item_id = pettingzoo.utils.counter_value(newpath)
		self.connection.acreate(id_to_token_path(self.path, item_id), "", zc.zk.OPEN_ACL_UNSAFE)
		if item_id > 0:
			children = self.connection.children(self.path + TOKEN_PATH)
			self._cleanup_tokens(children, item_id)
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
			try:
				self.ids.remove(removed_id)
				for callback in self.delete_callbacks:
					callback(self, removed_id)
			except:
				exc_class, exc, tb = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tb)
				raise exc_class, exc, tb

	def _on_new_id(self, new_id):
		"""
		Called internally when an item is added.  Not intended for external use.
		"""
		try:
			path = id_to_item_path(self.path, new_id)
			with self.write_lock:
				self.ids.add(new_id)
				deleted = Deleted(self.connection, path, [self._process_deleted])
				self.deletion_handlers[new_id] = deleted
				for callback in self.add_callbacks:
					callback(self, new_id)
		except:
			exc_class, exc, tb = sys.exc_info()
			sys.stderr.write(str(exc_class) + "\n")
			traceback.print_tb(tb)
			raise exc_class, exc, tb

	def _process_children_changed(self, children):
		"""
		Callback used for Children object.  Not intended for external use.
		"""
		try:
			new_max = pettingzoo.utils.max_counter(children)
			with self.write_lock:
				while self.max_token < new_max:
					self.max_token += 1
					self._on_new_id(self.max_token)
		except:
			exc_class, exc, tb = sys.exc_info()
			sys.stderr.write(str(exc) + "\n")
			traceback.print_tb(tb)
			raise exc_class, exc, tb

	def _process_deleted(self, node):
		"""
		Callback used for Deleted  object.  Not intended for external use.
		"""
		del_id = pettingzoo.utils.counter_value(node.path)
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
	return pettingzoo.utils.counter_path(path + ITEM_PATH + ITEM_PATH, item_id)

def id_to_token_path(path, item_id):
	"""
	Returns the full znode path for a token given item id. NOTE: Does not check validity of id!
	Parameters:
		item_id - for which to compute path
	Returns:
		znode path of node representing item
	"""
	return pettingzoo.utils.counter_path(path + TOKEN_PATH + TOKEN_PATH, item_id)
