import unittest
import k.config
import pettingzoo.discovery
import yaml
import time
from pettingzoo.utils import connect_to_zk

class DiscoveryTests(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_discovery'
		pettingzoo.discovery.CONFIG_PATH = self.path
		self.sample = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'localhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}

	def test_write_distributed_config(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		znode = self.connection.get(self.path + '/mysql/reports/127.0.0.1')
		config = yaml.load(znode[0])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_write_distributed_config_no_ip(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, interface='lo')
		znode = self.connection.get(self.path + '/mysql/reports/127.0.0.1')
		config = yaml.load(znode[0])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_remove_stale_config(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1', ephemeral=False)
		test = self.connection.exists(self.path + '/mysql/reports/127.0.0.1')
		self.assertTrue(test)
		pettingzoo.discovery.remove_stale_config(self.connection, 'mysql', 'reports', '127.0.0.1')
		test = self.connection.exists(self.path + '/mysql/reports/127.0.0.1')
		self.assertTrue(not test)

	def tearDown(self):
		pettingzoo.discovery.CONFIG_PATH = '/discovery'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)

class DistributedDiscoveryTests(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_discovery'
		pettingzoo.discovery.CONFIG_PATH = self.path
		self.sample = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'localhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}

	def test_distributed_config_dne(self):
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		self.assertRaises(IOError, dc.load_config, "doesn't", "exist")

	def test_load_config(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config('mysql', 'reports')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_load_config_via_path(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config_via_path('mysql/reports.yml')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_load_config_with_callback(self):
		self.touched = False
		self.cbpath = None
		self.cbconfig = None
		def cb(path, config):
			self.touched = True
			self.cbpath = path
			self.cbconfig = config
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config('mysql', 'reports', callback=cb)
		sample2 = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'notlocalhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', sample2, '127.0.0.2')
		time.sleep(.1)
		self.assertTrue(self.touched)
		self.assertEqual(self.cbpath, '/test_discovery/mysql/reports')
		config = self.cbconfig
		self.assertTrue(config['host'] in ['localhost', 'notlocalhost'])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		for key in mismatch_keys:
			self.assertTrue(key in ['host', 'metadata'])

	def test_load_config_with_multiple_entries(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		sample2 = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'notlocalhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', sample2, '127.0.0.2')
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config('mysql', 'reports')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		for key in mismatch_keys:
			self.assertTrue(key in ['host', 'metadata'])

	def tearDown(self):
		pettingzoo.discovery.CONFIG_PATH = '/discovery'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)

class DistributedMultiDiscovery(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_discovery'
		pettingzoo.discovery.CONFIG_PATH = self.path
		self.sample = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'localhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}
		self.sample2 = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'notlocalhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}

	def test_config_dne(self):
		dmc = pettingzoo.discovery.DistributedMultiDiscovery(self.connection)
		self.assertRaises(IOError, dmc.load_config, "doesn't", "exist")

	def test_load_config(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		dmc = pettingzoo.discovery.DistributedMultiDiscovery(self.connection)
		configs = dmc.load_config('mysql', 'reports')
		self.assertTrue(len(configs), 1)
		config = configs[0]
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_load_config_with_multiple_entries(self):
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample, '127.0.0.1')
		pettingzoo.discovery.write_distributed_config(self.connection, 'mysql', 'reports', self.sample2, '127.0.0.2')
		dmc = pettingzoo.discovery.DistributedMultiDiscovery(self.connection)
		configs = dmc.load_config('mysql', 'reports')
		self.assertTrue(len(configs), 2)
		ips = ['127.0.0.1', '127.0.0.2']
		for config in configs:
			mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
			ips.remove(config['metadata']['key'])
		self.assertEquals(len(ips), 0)

	def tearDown(self):
		pettingzoo.discovery.CONFIG_PATH = '/discovery'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)

class FileFallbackTests(unittest.TestCase):
	def setUp(self):
		self.mock = True
		self.conn_string = '127.0.0.1:2181'
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.setUp(self, connection_string=self.conn_string)
		self.connection = connect_to_zk(self.conn_string)
		self.path = '/test_discovery'
		pettingzoo.discovery.CONFIG_PATH = self.path
		self.sample = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'localhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}
		self.sample2 = {'metadata': {'service_class': 'mysql', 'version': 1.0}, 'username': 'reports', 'host': 'notlocalhost', 'port': 3306, 'password': 'reports', 'database': 'reports', 'encoding': 'utf8'}
		k.config.KnewtonConfig = k.config.KnewtonConfigTest()
		k.config.KnewtonConfig().add_config(self.sample, 'mysql/exist')
		self.list_sample = {'server_list': [self.sample]}
		k.config.KnewtonConfig().add_config(self.list_sample, 'mysql/list')
		self.list_sample2 = {'server_list': [self.sample, self.sample2]}
		k.config.KnewtonConfig().add_config(self.list_sample2, 'mysql/mlist')

	def test_config_file_fallback_dc(self):
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config('mysql', 'exist')
		self.assertEquals(config, self.sample)

	def test_config_file_list_fallback_dc(self):
		dc = pettingzoo.discovery.DistributedDiscovery(self.connection)
		config = dc.load_config('mysql', 'list')
		self.assertEquals(config, self.sample)
		config = dc.load_config('mysql', 'mlist')
		self.assertTrue(config in [self.sample, self.sample2])

	def test_config_file_fallback_dmc(self):
		dmc = pettingzoo.discovery.DistributedMultiDiscovery(self.connection)
		config = dmc.load_config('mysql', 'exist')
		self.assertEquals(config[0], self.sample)

	def test_config_file_fallback_list_dmc(self):
		dmc = pettingzoo.discovery.DistributedMultiDiscovery(self.connection)
		config = dmc.load_config('mysql', 'list')
		self.assertEquals(config[0], self.sample)
		config = dmc.load_config('mysql', 'mlist')
		self.assertEquals(config[0], self.sample)
		self.assertEquals(config[1], self.sample2)

	def tearDown(self):
		k.config.KnewtonConfig = k.config.KnewtonConfigDefault()
		pettingzoo.discovery.CONFIG_PATH = '/discovery'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()
		if self.mock:
			import zc.zk.testing
			zc.zk.testing.tearDown(self)

