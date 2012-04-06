import zc.zk
import zookeeper
import multiprocessing
import sys
import traceback
import pettingzoo.utils
import logging
from pettingzoo.deleted import Deleted

PREFIX = "/candidate"
logger = logging.getLogger('leader_queue')

class LeaderQueue(object):
	def __init__(self, connection, path):
		#zk connection
		self.connection = connection
		self.path = path
		self.deletion_handlers = {}
		# Ensures path exists
		self.connection.create_recursive(self.path + PREFIX, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.lock = multiprocessing.RLock()
		# Stores leader queues
		self.candidate_by_predecessor = {}
		self.counter_by_candidate = {}
			
	def remove_candidate(self, candidate):
		'''
		Removes candidate from zookeeper and updates leader
		queues to reflect deleted candidate
		Parameters:
			path - string. path to deleted node
		Returns:
			True if successful, False otherwise
		'''
		#create node in ZK. Node will be delete when backing framework is closed
		with self.lock:
			del_id = self.counter_by_candidate.get(candidate, None)
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
		Adds new candidate to zookeeper and updates
		leader queues to reflect new candidate
		Parameters:
			candidate - object that represents the candidate in the queue
			meta_data - binary data to store on node
		Returns:
			True if operation successfully completed. False otherwise
		'''
		#ensure candidate is not already in queue
		if self.has_candidate(candidate):
			print "Candidate already in queue."
			return False
		else:
			#create node in ZK. Node will be delete when backing framework is closed
			flags = zookeeper.EPHEMERAL | zookeeper.SEQUENCE
			newpath = self.connection.create(self.path + PREFIX + PREFIX, meta_data, zc.zk.OPEN_ACL_UNSAFE, flags)
			counter = pettingzoo.utils.counter_value(newpath)
			try:
				#create delete watch
				self._create_deletion_handlers(counter)
			except:
				exc_class, exc, tb = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tb)
				raise exc_class, exc, tb
			with self.lock:	
				self.counter_by_candidate[candidate] = counter
				self._handle_add(counter, candidate)
			return True

	def has_candidate(self, candidate):
		'''
		Checks to see if zookeeper knows about a candidate
		Parameters:
			candidate
		Returns:
			True if candidate exists, False if otherwise
		'''
		with self.lock:
			return self.counter_by_candidate.has_key(candidate)

	def _process_deleted(self, node):
		"""
		Callback used for Exists object.  Not intended for external use.
		"""
		del_id = pettingzoo.utils.counter_value(node.path)
		self._handle_remove(del_id) 
		with self.lock:
			del self.deletion_handlers[del_id]
	
	def _handle_remove(self, del_id): 
		'''
		called on delete. updates the candidate by predecessor dict
		Parameters:
			del_id - (int) id of deleted candidate
		Returns:
			None
		'''

		with self.lock:
			candidate = self.candidate_by_predecessor.get(del_id, None)
			if del_id == None: 
				print "Unknown candidate"
			elif candidate == None: 
				print "Removed candidate is not a predecessor"
			else:
				self._update_predecessor_dict(del_id, candidate)
				del self.candidate_by_predecessor[del_id]
		return None

	def _create_deletion_handlers(self, counter):
		'''
		called on create and update. creates delete watch on node 
		Parameters:
			counter	- (int) id of node to watch
		Returns:
			None
		'''
		with self.lock:
			# check watch does not already exist
			if self.deletion_handlers.get(counter, None) == None: 
				path = id_to_item_path(self.path, counter)
				delete_watch = Deleted(self.connection, path, [self._process_deleted])
				# stuff it on the object to make sure the watch still exists
				self.deletion_handlers[counter] = delete_watch
		return None

	def _handle_add(self, counter, candidate):
		'''
		called on node creation. 
		Parameters:
			counter - (int) counter of newly created candidate
			candidate - newly created candidate to add to predecessor
			dictionary
		Returns:
			None
		'''
		if counter == None:
			print "Unknown candidate."
		else:
			self._update_predecessor_dict(counter, candidate)
		return None


	def _update_predecessor_dict(self, counter, candidate):
		'''
		internal only. finds the predecessor of the candidate. updates the candidate by predecessor dict
		adds deletion handlers
		Parameters:
			counter - (int) counter of candidate you want to update in the queue
			candidate - candidate you want to add to predecessor queue 
		Returns:
			None
		'''
		try:
			# loop through children, check existence and counter
			children = self.connection.children(self.path + PREFIX)
			pred_id = pettingzoo.utils.min_predecessor(children, counter)
			with self.lock:
				self.candidate_by_predecessor[pred_id] = candidate
			if pred_id == -1: #congrats, you're it.
				candidate.on_elected()
			else:
				#find predecessor and set delete watch on it
				self._create_deletion_handlers(pred_id)
		except:
			exc_class, exc, tb = sys.exc_info()
			sys.stderr.write(str(exc_class) + "\n")
			traceback.print_tb(tb)
			raise exc_class, exc, tb
		return None

def id_to_item_path(path, item_id):
    """
    Returns the full znode path for a given item id. NOTE: Does not check validity of id!
    Parameters:
        item_id - for which to compute path
    Returns:
        znode path of node representing item
    """
    return pettingzoo.utils.counter_path(path + PREFIX + PREFIX, item_id)
