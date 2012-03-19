import unittest
import knewton.config
import pettingzoo.config
import yaml
import time
from pettingzoo.utils import connect_to_zk

class ConfigTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_config'
		pettingzoo.config.CONFIG_PATH = self.path
		self.sample = {'database': {'username': 'reports', 'host': 'localhost', 'password': 'reports', 'database': 'reports', 'adapter': 'mysql', 'encoding': 'utf8'}}

	def test_write_distributed_config(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		znode = self.connection.get(self.path + '/databases/reports/127.0.0.1')
		config = yaml.load(znode[0])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_write_distributed_config_no_ip(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, interface='lo')
		znode = self.connection.get(self.path + '/databases/reports/127.0.0.1')
		config = yaml.load(znode[0])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_remove_stale_config(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1', ephemeral=False)
		test = self.connection.exists(self.path + '/databases/reports/127.0.0.1')
		self.assertEqual(test['dataLength'], 119)
		pettingzoo.config.remove_stale_config(self.connection, 'databases', 'reports', '127.0.0.1')
		test = self.connection.exists(self.path + '/databases/reports/127.0.0.1')
		self.assertTrue(test == None)

	def tearDown(self):
		pettingzoo.config.CONFIG_PATH = '/config'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()

class DistributedConfigTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_config'
		pettingzoo.config.CONFIG_PATH = self.path
		self.sample = {'database': {'username': 'reports', 'host': 'localhost', 'password': 'reports', 'database': 'reports', 'adapter': 'mysql', 'encoding': 'utf8'}}

	def test_distributed_config_dne(self):
		dc = pettingzoo.config.DistributedConfig(self.connection)
		self.assertRaises(IOError, dc.load_config, "doesn't", "exist")

	def test_load_config(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.config.DistributedConfig(self.connection)
		(ip, config) = dc.load_config('databases', 'reports')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)
		self.assertEqual(ip, '127.0.0.1')

	def test_load_config_via_path(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.config.DistributedConfig(self.connection)
		(ip, config) = dc.load_config_via_path('databases/reports.yml')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)
		self.assertEqual(ip, '127.0.0.1')

	def test_load_config_with_callback(self):
		self.touched = False
		self.cbpath = None
		self.cbconfig = None
		def cb(path, config):
			self.touched = True
			self.cbpath = path
			self.cbconfig = config
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		dc = pettingzoo.config.DistributedConfig(self.connection)
		(ip, config) = dc.load_config('databases', 'reports', callback=cb)
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.2')
		time.sleep(.1)
		self.assertTrue(self.touched)
		self.assertEqual(self.cbpath, '/test_config/databases/reports')
		(ip, config) = self.cbconfig
		self.assertTrue(ip in ['127.0.0.1', '127.0.0.2'])
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)

	def test_load_config_with_multiple_entries(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.2')
		dc = pettingzoo.config.DistributedConfig(self.connection)
		(ip, config) = dc.load_config('databases', 'reports')
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)
		self.assertTrue(ip in ['127.0.0.1', '127.0.0.2'])

	def tearDown(self):
		pettingzoo.config.CONFIG_PATH = '/config'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()

class DistributedMultiConfigTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_config'
		pettingzoo.config.CONFIG_PATH = self.path
		self.sample = {'database': {'username': 'reports', 'host': 'localhost', 'password': 'reports', 'database': 'reports', 'adapter': 'mysql', 'encoding': 'utf8'}}
		self.sample2 = {'database': {'username': 'reports', 'host': 'notlocalhost', 'password': 'reports', 'database': 'reports', 'adapter': 'mysql', 'encoding': 'utf8'}}

	def test_config_dne(self):
		dmc = pettingzoo.config.DistributedMultiConfig(self.connection)
		self.assertRaises(IOError, dmc.load_config, "doesn't", "exist")

	def test_load_config(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		dmc = pettingzoo.config.DistributedMultiConfig(self.connection)
		configs = dmc.load_config('databases', 'reports')
		self.assertTrue(len(configs), 1)
		(ip, config) = configs[0]
		mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
		self.assertEqual(len(mismatch_keys), 0)
		self.assertEqual(ip, '127.0.0.1')

	def test_load_config_with_multiple_entries(self):
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.1')
		pettingzoo.config.write_distributed_config(self.connection, 'databases', 'reports', self.sample, '127.0.0.2')
		dmc = pettingzoo.config.DistributedMultiConfig(self.connection)
		configs = dmc.load_config('databases', 'reports')
		self.assertTrue(len(configs), 2)
		ips = ['127.0.0.1', '127.0.0.2']
		for (ip, config) in configs:
			mismatch_keys = [key for key in self.sample if not key in config or self.sample[key] != config[key]]
			self.assertEqual(len(mismatch_keys), 0)
			self.assertTrue(ip in ips)
			ips.remove(ip)
		self.assertEquals(len(ips), 0)

	def tearDown(self):
		pettingzoo.config.CONFIG_PATH = '/config'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()

class FileFallbackTests(unittest.TestCase):
	def setUp(self):
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.path = '/test_config'
		pettingzoo.config.CONFIG_PATH = self.path
		self.sample = {'database': {'username': 'reports', 'host': 'localhost', 'password': 'reports', 'database': 'reports', 'adapter': 'mysql', 'encoding': 'utf8'}}
		def fake_fetch_knewton_config(default, config=None):
			self.assertEquals('does/exist', default)
			return self.sample
		knewton.config._backup_fetch_knewton_config = knewton.config.fetch_knewton_config
		knewton.config.fetch_knewton_config = fake_fetch_knewton_config

	def test_config_file_fallback_dc(self):
		dmc = pettingzoo.config.DistributedConfig(self.connection)
		config = dmc.load_config('does', 'exist')
		self.assertEquals(config, self.sample)

	def test_config_file_fallback_dmc(self):
		dmc = pettingzoo.config.DistributedMultiConfig(self.connection)
		config = dmc.load_config('does', 'exist')
		self.assertEquals(config[0][0], 'file')
		self.assertEquals(config[0][1], self.sample)

	def tearDown(self):
		knewton.config.fetch_knewton_config = knewton.config._backup_fetch_knewton_config
		pettingzoo.config.CONFIG_PATH = '/config'
		self.connection.close()
		self.connection = connect_to_zk('127.0.0.1:2181')
		self.connection.delete_recursive(self.path)
		self.connection.close()

