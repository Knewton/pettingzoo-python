import unittest
from pettingzoo.dbag import *
from pettingzoo.utils import connect_to_zk
import time

class DbagTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_dbag'
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

	def test_dbag_get_items(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1]))

	def test_dbag_get_items_with_remove(self):
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		dbag.add("baz", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1, 2]))
		dbag.remove(1)
		children = [child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		self.assertTrue('item0000000002' in children)
		children = [child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000002' in children)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 2]))
		
	def test_dbag_add_listeners_add(self):
		self.touched = False
		def cb(dbag, e):
			self.touched = True
		dbag = DistributedBag(self.connection, self.path)
		dbag.add_listeners(add_callback=cb)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1]))
		self.assertTrue(self.touched)

	def test_dbag_add_listeners_remove(self):
		self.touched = False
		def cb(dbag, e):
			self.touched = True
		dbag = DistributedBag(self.connection, self.path)
		dbag.add_listeners(remove_callback=cb)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		dbag.add("baz", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1, 2]))
		dbag.remove(1)
		children = [child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		self.assertTrue('item0000000002' in children)
		children = [child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000002' in children)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 2]))
		self.assertTrue(self.touched)

	def test_max_counter(self):
		result = max_counter(["aasfgsfgsdfgsdfg-0000001001"])
		self.assertEquals(result, 1001)

	def test_counter_path(self):
		result = counter_path("aasfgsfgsdfgsdfg-", 1001)
		self.assertEquals(result, "aasfgsfgsdfgsdfg-0000001001")

	def test_id_to_item_path(self):
		self.assertEquals(id_to_item_path('/foo', 1354), '/foo/item/item0000001354')

	def test_id_to_token_path(self):
		self.assertEquals(id_to_token_path('/foo', 1354), '/foo/token/token0000001354')

	def tearDown(self):
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		try:
			self.connection.delete_recursive(self.path)
		except zookeeper.NoNodeException:
			pass # does not exist
		finally:
			self.connection.close()
