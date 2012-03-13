import unittest
from pettingzoo.dbag import *
from pettingzoo.config import connect_to_zk

class DbagTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test/dbag'
		self.created = False

	def test_dbag_create(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		children = [child for child in self.connection.children(self.path)]
		self.assertTrue('token' in children)
		self.assertTrue('item' in children)

	def test_dbag_add(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		id0 = dbag.add("foo", False)
		self.assertEquals(id0, 0)
		id1 = dbag.add("bar", False)
		self.assertEquals(id1, 1)
		children = [child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertTrue('item0000000001' in children)
		self.assertEquals(len(children), 2)
		children = [child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_dbag_remove(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		children = [child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertTrue('item0000000001' in children)
		result = dbag.remove(1)
		self.assertTrue(result)
		result = dbag.remove(2)
		self.assertFalse(result)
		children = [child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		children = [child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_dbag_get(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		result = dbag.get(0)
		self.assertEqual(result, "foo")
		result = dbag.get(1)
		self.assertEqual(result, None)

	def test_max_counter(self):
		result = max_counter(["aasfgsfgsdfgsdfg-0000001001"])
		self.assertEquals(result, 1001)

	def test_counter_path(self):
		result = counter_path("aasfgsfgsdfgsdfg-", 1001)
		self.assertEquals(result, "aasfgsfgsdfgsdfg-0000001001")

	def tearDown(self):
		if self.created:
			self.connection.delete_recursive(self.path)
