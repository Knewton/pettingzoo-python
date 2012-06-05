import zc.zk
import zookeeper
import multiprocessing
import sys
import traceback
import pettingzoo.utils
import logging
from abc import ABCMeta, abstractmethod
from pettingzoo.deleted import Deleted
from pettingzoo.utils import get_logger

PREFIX = "/candidate"
logger = logging.getLogger('leader_queue')

class LeaderQueue(object):
	def __init__(self, connection, path):
		#zk connection
		self.connection = connection
		self.path = path
		self.deletion_handlers = {}
		# Ensures path exists
		self.connection.create_recursive(
			self.path + PREFIX, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.lock = multiprocessing.RLock()
		# Stores leader queues
		self.candidate_by_predecessor = {}
		self.counter_by_candidate = {}
			
	def remove_candidate(self, candidate):
		'''
		Removes candidate from zookeeper and updates leader queues to
		reflect deleted candidate.

		:param path: string. path to deleted node
		:rtype: True if successful, False otherwise
		'''
		#create node in ZK. Node will be delete when backing framework is closed
		with self.lock:
			del_id = self.counter_by_candidate.get(candidate, None)
			get_logger().info("LeaderQueue.remove_candidate %s" % (del_id))
			if del_id == None:
				return False
		try:
			# delete node
			self.connection.delete(id_to_item_path(self.path, del_id))
			# update the leader queues
			self._handle_remove(del_id)
			#cleanup hashes
			with self.lock:
				del self.counter_by_candidate[candidate]
				return True
		except zookeeper.NoNodeException:
			return False 

	def add_candidate(self, candidate, meta_data=None):
		'''
		Adds new candidate to zookeeper and updates.
		Leader queues to reflect new candidate.

		:param candidate: object that represents the candidate in the queue
		:param meta_data: binary data to store on node
		:rtype: True if operation successfully completed. False otherwise
		'''
		#ensure candidate is not already in queue
		if self.has_candidate(candidate):
			get_logger().warning(
				"LeaderQueue.remove_candidate Candidate already in queue.")
			return False
		else:
			#create node in ZK. Node will be deleted when connection is closed
			flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE
			newpath = self.connection.create(
				self.path + PREFIX + PREFIX,
				meta_data,
				zc.zk.OPEN_ACL_UNSAFE,
				flags)
			counter = pettingzoo.utils.counter_value(newpath)
			get_logger().info("LeaderQueue.add_candidate %s" % (newpath))
			try:
				#create delete watch
				self._create_deletion_handlers(counter)
			except:
				exc_class, exc, tback = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tback)
				raise exc_class, exc, tback
			with self.lock:	
				self.counter_by_candidate[candidate] = counter
				self._handle_add(counter, candidate)
			return True

	def has_candidate(self, candidate):
		'''
		Checks to see if zookeeper knows about a candidate

		:param candidate:
		:rtype: True if candidate exists, False if otherwise
		'''
		with self.lock:
			return self.counter_by_candidate.has_key(candidate)

	def _process_deleted(self, node):
		"""
		Callback used for Exists object.
		Not intended for external use.
		"""
		del_id = pettingzoo.utils.counter_value(node.path)
		get_logger().debug("LeaderQueue._process_deleted %s" % (del_id))
		self._handle_remove(del_id) 
		with self.lock:
			del self.deletion_handlers[del_id]
	
	def _handle_remove(self, del_id): 
		'''
		Called on delete. Updates the candidate by predecessor dict.

		:param del_id: (int) id of deleted candidate
		:rtype: None
		'''

		with self.lock:
			candidate = self.candidate_by_predecessor.get(del_id, None)
			if del_id == None:
				get_logger().warning(
					"LeaderQueue._handle_remove Unknown candidate %s" %
					(del_id))
			elif candidate == None: 
				get_logger().debug(
					"LeaderQueue._handle_remove Removed candidate is not"
					+ " a predecessor %s" % (del_id))
			else:
				self._update_predecessor_dict(del_id, candidate)
				del self.candidate_by_predecessor[del_id]
		return None

	def _create_deletion_handlers(self, counter):
		'''
		Called on create and update. creates delete watch on node.

		:param counter: (int) id of node to watch
		:rtype: None
		'''
		with self.lock:
			# check watch does not already exist
			if self.deletion_handlers.get(counter, None) == None: 
				path = id_to_item_path(self.path, counter)
				delete_watch = Deleted(
					self.connection, path, [self._process_deleted])
				# stuff it on the object to make sure the watch still exists
				self.deletion_handlers[counter] = delete_watch
		return None

	def _handle_add(self, counter, candidate):
		'''
		Called on node creation. 

		:param counter: (int) counter of newly created candidate
		:param candidate: newly created candidate to add to predecessor \
		  dictionary
		:rtype: None
		'''
		if counter == None:
			get_logger().warning(
				"LeaderQueue._handle_add Unknown candidate %s" % (counter))
		else:
			self._update_predecessor_dict(counter, candidate)
		return None


	def _update_predecessor_dict(self, counter, candidate):
		"""
		Internal only. finds the predecessor of the candidate.
		Updates the candidate by predecessor dict adds deletion handlers.

		:param counter: (int) counter of candidate you want to update in \
		  the queue
		:param candidate: candidate you want to add to predecessor queue 
		:rtype: None
		"""
		try:
			# loop through children, check existence and counter
			children = self.connection.children(self.path + PREFIX)
			pred_id = pettingzoo.utils.min_predecessor(children, counter)
			with self.lock:
				self.candidate_by_predecessor[pred_id] = candidate
			if pred_id == -1:
				#congrats, you're it.
				candidate.on_elected()
			else:
				#find predecessor and set delete watch on it
				self._create_deletion_handlers(pred_id)
		except:
			exc_class, exc, tback = sys.exc_info()
			sys.stderr.write(str(exc_class) + "\n")
			traceback.print_tb(tback)
			raise exc_class, exc, tback
		return None

class Candidate:
	__metaclass__ = ABCMeta

	@abstractmethod
	def on_elected(self):
		'''
		should handle anything that needs to be done
		once a candidate has been elected leader
		'''

def id_to_item_path(path, item_id):
	"""
	Returns the full znode path for a given item id.
	NOTE: Does not check validity of id!

	:param item_id: - for which to compute path
	:rtype: znode path of node representing item
	"""
	return pettingzoo.utils.counter_path(path + PREFIX + PREFIX, item_id)
