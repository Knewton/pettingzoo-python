import unittest
from pettingzoo.dbag import *
from pettingzoo.utils import connect_to_zk, configure_logger
import time

class DbagTests(unittest.TestCase):
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
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_dbag'
		self.created = False
		self.touched = False
		configure_logger()

	def test_dbag_create(self):
		"""Tests that instantiating dbag creates all the needed paths."""
		DistributedBag(self.connection, self.path)
		self.created = True
		children = [child for child in self.connection.children(self.path)]
		self.assertTrue('token' in children)
		self.assertTrue('item' in children)

	def test_dbag_add(self):
		"""Tests that add actually adds the item to zookeeper."""
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		id0 = dbag.add("foo", False)
		self.assertEquals(id0, 0)
		id1 = dbag.add("bar", False)
		self.assertEquals(id1, 1)
		children = [
			child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertTrue('item0000000001' in children)
		self.assertEquals(len(children), 2)
		children = [
			child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_dbag_remove(self):
		"""Tests that remove actually removes the item from zookeeper."""
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		children = [
			child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertTrue('item0000000001' in children)
		result = dbag.remove(1)
		self.assertTrue(result)
		result = dbag.remove(2)
		self.assertFalse(result)
		children = [
			child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		children = [
			child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000001' in children)
		self.assertEquals(len(children), 1)

	def test_dbag_get(self):
		"""
		Tests that calling get returns you the data payload of the item you
		call it on.
		"""
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		result = dbag.get(0)
		self.assertEqual(result, "foo")
		result = dbag.get(1)
		self.assertEqual(result, None)

	def test_dbag_get_items(self):
		"""Tests that calling get_items returns you all item ids."""
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1]))

	def test_dbag_get_items_with_remove(self):
		"""
		Verifies that items removed are properly reflected from the dbag.
		"""
		dbag = DistributedBag(self.connection, self.path)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		dbag.add("baz", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1, 2]))
		dbag.remove(1)
		children = [
			child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		self.assertTrue('item0000000002' in children)
		children = [
			child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000002' in children)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 2]))
		
	def test_dbag_add_listeners_add(self):
		"""
		tests that adding items causes them to be properly added to the list.
		Also verifies that the add callback gets hit.
		"""
		dbag = DistributedBag(self.connection, self.path)
		def callback(idbag, bag_id):
			"""Tests that the input the callback is appropriate"""
			self.assertEqual(dbag, idbag)
			self.assertTrue(bag_id in [0, 1])
			self.touched = True
		dbag.add_listeners(add_callback=callback)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1]))
		self.assertTrue(self.touched)

	def test_dbag_add_listeners_remove(self):
		"""
		Tests that the removing items causes them to properly be removed
		from the list.  Also verifies that the remove callback gets hit.
		"""
		dbag = DistributedBag(self.connection, self.path)
		def callback(idbag, bag_id):
			"""Tests that the input the callback is appropriate"""
			self.assertEqual(dbag, idbag)
			self.assertTrue(bag_id in [1])
			self.touched = True
		dbag.add_listeners(remove_callback=callback)
		self.created = True
		dbag.add("foo", False)
		dbag.add("bar", False)
		dbag.add("baz", False)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 1, 2]))
		dbag.remove(1)
		children = [
			child for child in self.connection.children(self.path + "/item")]
		self.assertTrue('item0000000000' in children)
		self.assertFalse('item0000000001' in children)
		self.assertTrue('item0000000002' in children)
		children = [
			child for child in self.connection.children(self.path + "/token")]
		self.assertTrue('token0000000002' in children)
		time.sleep(0.25)
		self.assertEqual(dbag.get_items(), set([0, 2]))
		self.assertTrue(self.touched)

	def test_max_counter(self):
		"""
		Tests that pettingzoo.utils.max_counter properly returns the integer
		id from an input counter path.
		"""
		result = pettingzoo.utils.max_counter(["aasfgsfgsdfgsdfg-0000001001"])
		self.assertEquals(result, 1001)

	def test_counter_path(self):
		"""
		Tests that pettingzoo.utils.counter_path returns an appropriate
		counter path.
		"""
		result = pettingzoo.utils.counter_path("aasfgsfgsdfgsdfg-", 1001)
		self.assertEquals(result, "aasfgsfgsdfgsdfg-0000001001")

	def test_id_to_item_path(self):
		""" Tests that id_to_item_path returns an appropriate item path."""
		self.assertEquals(
			id_to_item_path('/foo', 1354),
			'/foo/item/item0000001354')

	def test_id_to_token_path(self):
		""" Tests that id_to_token_path returns an appropriate token path."""
		self.assertEquals(
			id_to_token_path('/foo', 1354),
			'/foo/token/token0000001354')

	def tearDown(self):
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		try:
			self.connection.delete_recursive(self.path)
		except zookeeper.NoNodeException:
			pass # does not exist
		finally:
			self.connection.close()
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)
