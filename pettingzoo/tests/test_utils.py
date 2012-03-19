import unittest
from pettingzoo.utils import *
import sys

class ExistsTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_exists'

	def test_exists_watch(self):
		self.touched = False
		def cb(e):
			self.touched = True
		test_path = self.path + "/exists"
		self.connection.create_recursive(test_path, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		exists = Exists(self.connection, test_path)
		exists(cb)
		self.connection.delete_recursive(test_path)
		self.assertTrue(self.touched)

	def tearDown(self):
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		try:
			self.connection.delete_recursive(self.path)
		except zookeeper.NoNodeException:
			pass # does not exist
		finally:
			self.connection.close()
