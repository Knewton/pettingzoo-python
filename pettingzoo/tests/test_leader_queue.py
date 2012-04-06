import unittest
from pettingzoo.leader_queue import *
import pettingzoo.utils
from pettingzoo.candidate import Candidate
import time
import sys

class TestCandidate(Candidate):
	def on_elected(self):
		pass
	def on_close(self):
		pass

class LeaderQueueTests(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			import zc.zk.testing
			import pettingzoo.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
			zc.zk.testing.ZooKeeper.create = pettingzoo.testing.create
			zc.zk.testing.ZooKeeper.exists = pettingzoo.testing.exists
			zc.zk.testing.Node.deleted = pettingzoo.testing.deleted
		self.connection = pettingzoo.utils.connect_to_zk(self.conn_string)
		self.path = '/test_leaderq'
		self.created = False

	def test_lq_create(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		children = [child for child in self.connection.children(self.path)]
		self.assertTrue('candidate' in children)

	def test_lq_add_candidate(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		candidates = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 2)

	def test_lq_remove(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		cs = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertTrue(leaderq.remove_candidate(cs[0]))
		self.assertFalse(leaderq.remove_candidate(TestCandidate()))
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertFalse('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_predecessor(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		cs = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertTrue(leaderq.remove_candidate(cs[0]))
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertFalse('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_predecessor_update_on_delete(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		cs = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertEquals(len(children), 2)
		self.assertTrue('candidate0000000000' in children)
		self.assertTrue('candidate0000000001' in children)
		self.assertEqual(len(leaderq.counter_by_candidate.keys()), 2)
		self.assertEqual(leaderq.counter_by_candidate[cs[0]], 0)
		self.assertEqual(leaderq.counter_by_candidate[cs[1]], 1)
		self.assertEqual(leaderq.candidate_by_predecessor.keys(), [0, -1]) 
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cs[0])
		self.assertEqual(leaderq.candidate_by_predecessor[0], cs[1]) 
		self.assertTrue(leaderq.remove_candidate(cs[0]))
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cs[1])
		self.assertEqual(leaderq.counter_by_candidate[cs[1]], 1)
		children = [child for child in self.connection.children(self.path + "/candidate")]

	def test_predecessor_update_on_add(self):
		leaderq = LeaderQueue(self.connection, self.path)
		self.created = True
		cs = self._create_candidates(leaderq, num_candidates=2)
		children = [child for child in self.connection.children(self.path + "/candidate")]
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cs[0])
		self.assertEqual(leaderq.candidate_by_predecessor[0], cs[1]) 
		self.assertTrue(leaderq.remove_candidate(cs[0]))
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cs[1])
		self.assertEqual(leaderq.counter_by_candidate[cs[1]], 1)
		cs.extend(self._create_candidates(leaderq, num_candidates=2))
		self.assertEqual(leaderq.counter_by_candidate[cs[3]], 3)
		self.assertEqual(leaderq.candidate_by_predecessor[-1], cs[1])
		self.assertTrue(leaderq.remove_candidate(cs[1]))
		self.assertEqual(leaderq.candidate_by_predecessor[2], cs[3])

	def test_id_to_item_path(self):
		self.assertEquals(id_to_item_path('/foo', 1354), '/foo/candidate/candidate0000001354')

	def _create_candidates(self, leaderq, num_candidates):
		'''
		convenience method. creates a bunch of candidates and adds them to the
		leader queue.
		Returns:
			list of candidates
		'''
		candidates = []
		for i in range(0, num_candidates):
			c = TestCandidate()
			self.assertTrue(leaderq.add_candidate(c))
			candidates.append(c)
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
			import zc.zk.testing
			zc.zk.testing.tearDown(self)
