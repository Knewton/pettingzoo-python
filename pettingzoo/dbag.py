"""
.. module:: pettingzoo.dbag
.. moduleauthor:: Devon Jones <devon@knewton.com>

:platform: Unix

:synopsis: Pettingzoo distributed bag  implements a zookeeper protocol \
designed to create event driven data collections that can be shared between \
arbitrary processes.

pettingzoo.dbag allows a service or a program to store information in a
distributed bag that will fire events to all participants upon any change
to the bag's contents.
"""
import zc.zk
import zookeeper
import sys
import traceback
import pettingzoo.utils
from pettingzoo.deleted import Deleted
from pettingzoo.utils import get_logger

ITEM_PATH = "/item"
TOKEN_PATH = "/token"

class DistributedBag(object):
	"""
	DistributedBag is a class that uses zookeeper to be able to create and
	manage the distributed bag protocol.  This can be used to create small
	databases of items like subscriptions from one process to another.
	The advantage over other databases in this use case is that this allows
	all participants to have triggered events take place upon content changes
	in the bag.

	:param connection: a zc.zk.ZooKeeper connection
	:param path: zookeeper path of the distributed bag

	**Note**

	This protocol uses two kinds of callback.  Add callbacks and Remove
	callbacks.  These are called when an item is added to the bag and when
	an item is removed from the bag respectivly.
	Callbacks should be in the form of some_callback(dbag, id) where dbag is
	the instance of this class, and id is the bag id of the element that has
	been added or removed.
	"""
	def __init__(self, connection, path):
		self.connection = connection
		self.path = path
		self.connection.create_recursive(
			path + ITEM_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.connection.create_recursive(
			path + TOKEN_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.lock = pettingzoo.utils.ReadWriteLock()
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
		"""Fills out the bag when initial connection to it is made"""
		ichildren = self.connection.children(self.path + ITEM_PATH)
		with self.lock:
			for child in ichildren:
				try:
					new_id = pettingzoo.utils.counter_value(child)
					path = id_to_item_path(self.path, new_id)
					get_logger().info("DistributedBag._on_new_id %s" % (path))
					self.ids.add(new_id)
					deleted = Deleted(
						self.connection, path, [self._process_deleted])
					self.deletion_handlers[new_id] = deleted
					for callback in self.add_callbacks:
						callback(self, new_id)
				except:
					exc_class, exc, tback = sys.exc_info()
					sys.stderr.write(str(exc_class) + "\n")
					traceback.print_tb(tback)
					raise exc_class, exc, tback

	def _cleanup_tokens(self, children, max_token):
		"""
		Tokens are used to track the current max token only.  This
		substantially lowers the impact on zookeeper when items are
		added as opposed to tracking children in the /items path,
		especially when /items becomes large.  This method removes
		any errant tokens if it sees any tokens still in the system
		smaller then max_token.
		"""
		for child in children:
			token_id = pettingzoo.utils.counter_value(child)
			if token_id < max_token:
				get_logger().warning(
					"DistributedBag._cleanup_tokens %s" % (token_id))
				try:
					self.connection.adelete(
						id_to_token_path(self.path, token_id))
				except zookeeper.NoNodeException:
					pass # If it doesn't exist, that's ok

	def add(self, data, ephemeral=True):
		"""
		Inserts an element into the bag.

		:param data: value stored at element
		:param ephemeral: if true, element is automatically deleted when \
		  backing framework is closed
		:rtype: *int* element id (required for deletion)
		"""
		flags = zookeeper.SEQUENCE
		if ephemeral:
			flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE
		newpath = self.connection.create(
			self.path + ITEM_PATH + ITEM_PATH,
			data, zc.zk.OPEN_ACL_UNSAFE, flags)
		item_id = pettingzoo.utils.counter_value(newpath)
		get_logger().debug("DistributedBag.add %s: %s" % (item_id, data))
		self.connection.acreate(
			id_to_token_path(self.path, item_id), "", zc.zk.OPEN_ACL_UNSAFE)
		if item_id > 0:
			children = self.connection.children(self.path + TOKEN_PATH)
			self._cleanup_tokens(children, item_id)
		return item_id

	def remove(self, item_id):
		"""
		Remove an item from the bag.

		:param item_id: of node to deleteid_to_item_path(path, item_id) \
		  (returned by addElement)
		:rtype: *boolean* true if node was deleted, false if node did not exist
		"""
		try:
			get_logger().debug("DistributedBag.remove %s" % item_id)
			self.connection.delete(id_to_item_path(self.path, item_id))
			return True
		except zookeeper.NoNodeException:
			return False

	def get(self, item_id):
		"""
		Returns the data payload of a specific item_id's znode

		:param item_id: to retrieve
		:rtype: data stored at id (or absent if invalid id)
		"""
		try:
			get_logger().debug("DistributedBag.get %s" % item_id)
			return self.connection.get(id_to_item_path(self.path, item_id))[0]
		except zookeeper.NoNodeException:
			return None

	def add_listeners(self, add_callback=None, remove_callback=None):
		"""
		Allows caller to add callbacks for item addition and item removal.

		:param add_callback: (Optional) callback that will be called when an \
		  id is created.
		:param remove_callback: (Optional) callback that will be called when \
		  an id is created.

		Callbacks are expected to have the following interface:
		Parameters:

		* dbag - This object
		* affected_id - The id that is being added or deleted, as appropriate \
		  to the call
		"""
		with self.lock:
			if add_callback:
				get_logger().debug(
					"DistributedBag.add_listeners added add listener")
				self.add_callbacks.append(add_callback)
			if remove_callback:
				get_logger().debug(
					"DistributedBag.add_listeners added remove listener")
				self.delete_callbacks.append(remove_callback)
			return self.get_items()

	def get_items(self):
		"""
		Returns a set of presently existant item ids.

		:rtype: set of integers
		"""
		try:
			self.lock.acquire_read()
			return self.ids.copy()
		finally:
			self.lock.release_read()

	def _on_delete_id(self, removed_id):
		"""
		Called internally when an item is deleted.
		**Not intended for external use.**
		"""
		path = id_to_item_path(self.path, removed_id)
		get_logger().info("DistributedBag._on_delete_id %s" % (path))
		with self.lock:
			try:
				self.ids.remove(removed_id)
				for callback in self.delete_callbacks:
					callback(self, removed_id)
			except:
				exc_class, exc, tback = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tback)
				raise exc_class, exc, tback

	def _on_new_id(self, new_id):
		"""
		Called internally when an item is added.
		**Not intended for external use.**
		"""
		try:
			path = id_to_item_path(self.path, new_id)
			get_logger().info("DistributedBag._on_new_id %s" % (path))
			with self.lock:
				self.ids.add(new_id)
				deleted = Deleted(
					self.connection, path, [self._process_deleted])
				self.deletion_handlers[new_id] = deleted
				for callback in self.add_callbacks:
					callback(self, new_id)
		except:
			exc_class, exc, tback = sys.exc_info()
			sys.stderr.write(str(exc_class) + "\n")
			traceback.print_tb(tback)
			raise exc_class, exc, tback

	def _process_children_changed(self, children):
		"""
		Callback used for Children object.
		**Not intended for external use.**
		"""
		try:
			new_max = pettingzoo.utils.max_counter(children)
			get_logger().debug(
				"DistributedBag._process_children_changed %s" % (new_max))
			with self.lock:
				while self.max_token < new_max:
					self.max_token += 1
					self._on_new_id(self.max_token)
		except:
			exc_class, exc, tback = sys.exc_info()
			sys.stderr.write(str(exc) + "\n")
			traceback.print_tb(tback)
			raise exc_class, exc, tback

	def _process_deleted(self, node):
		"""
		Callback used for Deleted object.
		**Not intended for external use.**
		"""
		del_id = pettingzoo.utils.counter_value(node.path)
		get_logger().debug(
			"DistributedBag._process_deleted %s" % (del_id))
		self._on_delete_id(del_id)
		with self.lock:
			del self.deletion_handlers[del_id]
        

def id_to_item_path(path, item_id):
	"""
	Returns the full znode path for a given item id.
	NOTE: Does not check validity of id!

	:param item_id: for which to compute path
	:rtype: *str* znode path of node representing item
	"""
	return pettingzoo.utils.counter_path(
		path + ITEM_PATH + ITEM_PATH, item_id)

def id_to_token_path(path, item_id):
	"""
	Returns the full znode path for a token given item id.
	NOTE: Does not check validity of id!

	:param item_id: for which to compute path
	:rtype: *str* znode path of node representing item
	"""
	return pettingzoo.utils.counter_path(
		path + TOKEN_PATH + TOKEN_PATH, item_id)
