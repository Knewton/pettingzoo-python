import unittest
import pettingzoo.local_config
import os

class ConfigDefaultsTest(unittest.TestCase):
	def setUp(self):
		self.orig = pettingzoo.local_config.LocalConfigPath

	def test_default_configs(self):
		prefixes = pettingzoo.local_config.LocalConfigPath().prefixes
		self.assertTrue("" in prefixes)
		self.assertTrue(os.path.join('~', '.pettingzoo') in prefixes)
		self.assertTrue('/etc/pettingzoo/' in prefixes)
		self.assertEqual(len(prefixes), 3)

	def test_overload_config_defaults(self):
		pettingzoo.local_config.LocalConfigPath = pettingzoo.local_config.LocalConfigPathDefaults(
			["./pettingzoo/tests/configs"])
		prefixes = pettingzoo.local_config.LocalConfigPath().prefixes
		self.assertTrue("./pettingzoo/tests/configs" in prefixes)
		self.assertEqual(len(prefixes), 1)

	def tearDown(self):
		pettingzoo.local_config.LocalConfigPath = self.orig

class ConfigTests(unittest.TestCase):
	def setUp(self):
		self.orig = pettingzoo.local_config.LocalConfigPath
		pettingzoo.local_config.LocalConfigPath = pettingzoo.local_config.LocalConfigPathDefaults(
			[os.path.abspath("pettingzoo/tests/configs")])
		pettingzoo.local_config.LocalConfig.config_types = {}

	def test_find_local_config_path(self):
		path = pettingzoo.local_config.find_local_config_path('databases/reports')
		parts = path.split("/")
		self.assertEqual(parts[-1], 'reports.yml')
		self.assertEqual(parts[-2], 'databases')
		self.assertRaises(IOError, pettingzoo.local_config.find_local_config_path, 'databases/foo')

	def test_fetch_local_config(self):
		payload = pettingzoo.local_config.fetch_local_config('memcached/sessions.yml')
		self.assertTrue('memcache' in payload.keys())
		self.assertEqual(len(payload.keys()), 1)
		self.assertTrue('namespace' in payload['memcache'].keys())
		self.assertEqual('test', payload['memcache']['namespace'])
		self.assertTrue('port' in payload['memcache'].keys())
		self.assertEqual(11211, payload['memcache']['port'])
		self.assertTrue('address' in payload['memcache'].keys())
		self.assertEqual('localhost', payload['memcache']['address'])
		self.assertEqual(len(payload['memcache'].keys()), 3)
		self.assertRaises(IOError, pettingzoo.local_config.fetch_local_config, 'databases/foo')

	def test_override_fetch_local_config(self):
		payload = pettingzoo.local_config.fetch_local_config('databases/reports', 'memcached/sessions.yml')
		self.assertTrue('memcache' in payload.keys())
		self.assertEqual(len(payload.keys()), 1)
		self.assertTrue('namespace' in payload['memcache'].keys())
		self.assertEqual('test', payload['memcache']['namespace'])
		self.assertTrue('port' in payload['memcache'].keys())
		self.assertEqual(11211, payload['memcache']['port'])
		self.assertTrue('address' in payload['memcache'].keys())
		self.assertEqual('localhost', payload['memcache']['address'])
		self.assertEqual(len(payload['memcache'].keys()), 3)
		self.assertRaises(IOError, pettingzoo.local_config.fetch_local_config, 'databases/foo')

	def test_local_config(self):
		payload = pettingzoo.local_config.LocalConfig().fetch_config('memcached/sessions.yml')
		self.assertTrue('memcache' in payload.keys())
		self.assertEqual(len(payload.keys()), 1)
		self.assertTrue('namespace' in payload['memcache'].keys())
		self.assertEqual('test', payload['memcache']['namespace'])
		self.assertTrue('port' in payload['memcache'].keys())
		self.assertEqual(11211, payload['memcache']['port'])
		self.assertTrue('address' in payload['memcache'].keys())
		self.assertEqual('localhost', payload['memcache']['address'])
		self.assertEqual(len(payload['memcache'].keys()), 3)

		cache = pettingzoo.local_config.LocalConfig().config_types
		self.assertTrue('memcached/sessions.yml__None' in cache.keys())
		print cache.keys()
		self.assertEqual(len(cache.keys()), 1)
		self.assertTrue('memcache' in cache['memcached/sessions.yml__None'].keys())
		self.assertEqual(len(cache.keys()), 1)

		payload2 = pettingzoo.local_config.LocalConfig().fetch_config('memcached/sessions.yml')
		self.assertEqual(cache['memcached/sessions.yml__None'], payload2)

	def test_discovery(self):
		payload = pettingzoo.local_config.LocalConfig().fetch_discovery('mysql', 'reports')
		self.assertTrue('server_list' in payload.keys())
		server_list = payload['server_list']
		self.assertEqual(len(server_list), 1)
		config = server_list[0]
		self.assertTrue('header' in config.keys())
		header = config['header']
		self.assertEqual(header['service_class'], 'mysql')
		self.assertEqual(header['metadata']['protocol'], 'mysql')
		self.assertEqual(header['metadata']['version'], 1.0)
		self.assertEqual(config['encoding'], 'utf8')
		self.assertEqual(config['database'], 'reports')
		self.assertEqual(config['username'], 'reports')
		self.assertEqual(config['password'], 'reports')
		self.assertEqual(config['host'], 'localhost')
		
	def test_discovery_no_server_list(self):
		payload = pettingzoo.local_config.LocalConfig().fetch_config('discovery/mysql/knewmena.yml')
		self.assertTrue(not payload.has_key('server_list'))
		payload = pettingzoo.local_config.LocalConfig().fetch_discovery('mysql', 'knewmena')
		self.assertTrue('server_list' in payload.keys())
		server_list = payload['server_list']
		self.assertEqual(len(server_list), 1)
		config = server_list[0]
		self.assertTrue('header' in config.keys())
		header = config['header']
		self.assertEqual(header['service_class'], 'mysql')
		self.assertEqual(header['metadata']['protocol'], 'mysql')
		self.assertEqual(header['metadata']['version'], 1.0)
		self.assertEqual(config['encoding'], 'utf8')
		self.assertEqual(config['database'], 'knewmena')
		self.assertEqual(config['username'], 'knewmena')
		self.assertEqual(config['password'], 'knewmena')
		self.assertEqual(config['host'], 'localhost')
		
	def tearDown(self):
		pettingzoo.local_config.LocalConfigPath = self.orig

class KnewtonConfigTestTests(unittest.TestCase):
	def setUp(self):
		self.orig = pettingzoo.local_config.LocalConfigPath
		pettingzoo.local_config.LocalConfigPath = pettingzoo.local_config.LocalConfigPathDefaults(
			[os.path.abspath("config/tests/configs")])
		self.cache = {
				'memcached/sessions.yml__None': {
					'memcache': {
						'namespace': 'test', 'port': 11211, 'address': 'localhost'
					}
				}
			}

	def test_override(self):
		pettingzoo.local_config.LocalConfig = pettingzoo.local_config.LocalConfigTest(self.cache)
		payload = pettingzoo.local_config.LocalConfig().fetch_config('memcached/sessions.yml')
		self.assertTrue('memcache' in payload.keys())
		self.assertEqual(len(payload.keys()), 1)
		self.assertTrue('namespace' in payload['memcache'].keys())
		pettingzoo.local_config.LocalConfig().add_config({}, 'databases/reports.yml')
		payload = pettingzoo.local_config.LocalConfig().fetch_config('databases/reports.yml')
		self.assertEqual(len(payload.keys()), 0)

	def tearDown(self):
		pettingzoo.local_config.LocalConfigPath = self.orig
		pettingzoo.local_config.LocalConfig = pettingzoo.local_config.LocalConfigDefault()

