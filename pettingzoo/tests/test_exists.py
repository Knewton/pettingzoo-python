import unittest
import zookeeper
from pettingzoo.utils import connect_to_zk
import pettingzoo.exists
import zc
from pettingzoo.dbag import DistributedBag

ZOO_CONF = "/etc/knewton/zookeeper/platform.yml"

DO_MOCK = True

class ExistsTests(unittest.TestCase):
	def setUp(self):
		self.conn_string = '127.0.0.1:2181'
		if DO_MOCK:
			import pettingzoo.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
			zc.zk.testing.ZooKeeper.create = pettingzoo.testing.create
			zc.zk.testing.ZooKeeper.exists = pettingzoo.testing.exists
			zc.zk.testing.Node.deleted = pettingzoo.testing.deleted
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_exists'

	def test_exists_watch(self):
		self.touched = False
		def cb(e):
			self.touched = True
		test_path = self.path + "/exists"
		self.connection.create_recursive(test_path, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		exists = pettingzoo.exists.Exists(self.connection, test_path)
		exists(cb)
		self.connection.delete_recursive(test_path)
		self.assertTrue(self.touched)

	def test_shared_connection(self):
		if not DO_MOCK:
			DistributedBag(
				self.connection, '/altnode/organ/testorgan/substrate/subscription')
			DistributedBag(
				self.connection, '/altnode/organ/testorgan/substrate/subscription')
		else:
			print "Skipped: test_shared_connection requires a running ZK instance"

	def tearDown(self):
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		try:
			self.connection.delete_recursive(self.path)
		except zookeeper.NoNodeException:
			pass # does not exist
		finally:
			self.connection.close()
		if DO_MOCK:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)
