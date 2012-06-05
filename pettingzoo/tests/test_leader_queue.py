import unittest
import threading
import zc.zk.testing
import pettingzoo.utils
import pettingzoo.testing
from pettingzoo.leader_queue import *

class TestCandidate(Candidate):
	def on_elected(self):
		pass

class LeaderQueueTests(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
			zc.zk.testing.ZooKeeper.create = pettingzoo.testing.create
			zc.zk.testing.ZooKeeper.exists = pettingzoo.testing.exists
			zc.zk.testing.Node.deleted = pettingzoo.testing.deleted
		self.connection = pettingzoo.utils.connect_to_zk(self.conn_string)
		self.path = '/test_leaderq'
		pettingzoo.utils.configure_logger()

	def test_lq_create(self):
		"""
		Tests that creation of a leader queue creates the appropriate needed
		paths on zookeeper.
		"""
		LeaderQueue(self.connection, self.path)
		children = [child for child in self.connection.children(self.path)]
		self.assertTrue('candidate' in children)

	def test_lq_add_candidate(self):
		"""
		Tests that adding a candidate to the leader queue is reflected in
		zookeeper
		"""
		leaderq = LeaderQueue(self.connection, self.path)
		self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 2)

	def test_lq_remove(self):
		"""
		Tests that removing a candidate from the leader queue is reflected
		in zookeeper
		"""
		leaderq = LeaderQueue(self.connection, self.path)
		cands = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertTrue(leaderq.remove_candidate(cands[0]))
		self.assertFalse(leaderq.remove_candidate(TestCandidate()))
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertFalse('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_predecessor(self):
		"""
		"""
		leaderq = LeaderQueue(self.connection, self.path)
		cands = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertTrue(leaderq.remove_candidate(cands[0]))
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertFalse('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_predecessor_update_on_delete(self):
		"""
		Tests that the leader queue object is properly updated
		when a candidate is added.
		"""
		leaderq = LeaderQueue(self.connection, self.path)
		cands = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEqual(len(leaderq.counter_by_candidate.keys()), 2)
		self.assertEqual(leaderq.counter_by_candidate[cands[0]], 0)
		self.assertEqual(leaderq.counter_by_candidate[cands[1]], 1)
		self.assertEqual(leaderq.candidate_by_predecessor.keys(), [0, -1])
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cands[0])
		self.assertEqual(leaderq.candidate_by_predecessor[0], cands[1]) 
		self.assertTrue(leaderq.remove_candidate(cands[0]))
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cands[1])
		self.assertEqual(leaderq.counter_by_candidate[cands[1]], 1)

	def test_predecessor_update_on_add(self):
		"""
		Tests that the leader queue object is properly updated
		when a candidate is added.
		"""
		leaderq = LeaderQueue(self.connection, self.path)
		cands = self._create_candidates(leaderq, num_candidates=2)
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cands[0])
		self.assertEqual(leaderq.candidate_by_predecessor[0], cands[1]) 
		self.assertTrue(leaderq.remove_candidate(cands[0]))
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cands[1])
		self.assertEqual(leaderq.counter_by_candidate[cands[1]], 1)
		cands.extend(self._create_candidates(leaderq, num_candidates=2))
		self.assertEqual(leaderq.counter_by_candidate[cands[3]], 3)
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cands[1])
		self.assertTrue(leaderq.remove_candidate(cands[1]))
		self.assertEqual(leaderq.candidate_by_predecessor[2], cands[3])

	def test_on_elected(self):
		"""
		Tests that on_elected gets called on the candidate when
		something else removes all predecessors.
		"""
		self.touched = False
		event = threading.Event()
		class TouchedCandidate(Candidate):
			def on_elected(slf):
				event.set()
				self.touched = True
		leaderq = LeaderQueue(self.connection, self.path)
		cands = self._create_candidates(leaderq, num_candidates=1)
		tcand = TouchedCandidate()
		leaderq.add_candidate(tcand)
		children = [child for child in self.connection.children(
			self.path + "/candidate")]
		for child in children:
			self.connection.delete(self.path + "/candidate/" + str(child))
		event.wait(0.25)
		self.assertTrue(self.touched)

	def test_id_to_item_path(self):
		"""
		Tests that id_to_item_path returns an appropriate path
		"""
		self.assertEquals(
			id_to_item_path('/foo', 1354),
			'/foo/candidate/candidate0000001354')

	def _create_candidates(self, leaderq, num_candidates):
		'''
		convenience method. creates a bunch of candidates and adds them to the
		leader queue.
		Returns:
			list of candidates
		'''
		candidates = []
		for _ in range(0, num_candidates):
			cand = TestCandidate()
			self.assertTrue(leaderq.add_candidate(cand))
			candidates.append(cand)
		return candidates

	def tearDown(self):
		self.connection.close()
		self.connection = pettingzoo.utils.connect_to_zk('127.0.0.1:2181')
		try:
			self.connection.delete_recursive(self.path)
		except zookeeper.NoNodeException:
			pass # does not exist
		finally:
			self.connection.close()
		if self.mock:
			zc.zk.testing.tearDown(self)
