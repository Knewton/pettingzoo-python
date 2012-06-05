"""
Classes and functions that implement the config paradigm for PettingZoo.
"""

import yaml
import os

class LocalConfigPathDefaults(object):
	"""
	This class is a singleton intended to hold the paths that will be looked at,
	in order, for finding config files.
	If you want to override the defaults of [".", "~/.pettingzoo",
	"/etc/pettingzoo"], do the following:
	import pettingzoo.local_config
	pettingzoo.local_config.LocalConfigPath = pettingzoo.local_config.LocalConfigPathDefaults(
	  [os.path.abspath("config/tests/configs")])
	"""
	def __init__(self, pathlist=[
			"",
			os.path.join('~', '.pettingzoo'),
			'/etc/pettingzoo/']):
		self.prefixes = pathlist

	def __call__(self):
		return self

LocalConfigPath = LocalConfigPathDefaults()

def find_local_config_path(file_name):
	"""
	Not intended for calling outside of this module.
	This function will look in all paths, in order
	for the requested file both with and without .yml
	Parameters:
	 - file_name: the file name to search for.
	Raises:
	 - IOError if no file is found
	"""
	for prefix in LocalConfigPath().prefixes:
		file_path = os.path.expanduser(os.path.join(prefix, file_name))
		if os.path.exists(file_path):
			return file_path
		if os.path.exists(file_path + ".yml"):
			return file_path + ".yml"
	raise IOError("Config file %s does not exist" % (file_name))

def fetch_local_config(default, config=None):
	"""
	Returns the content of a yml config file as a hash
	Parameters:
	 - default: default file name to look for
	 - config: override with this file name instead. (optional)
	   Note: the pattern of using config is intended to make using this with
	   OptionsParser easier.  Otherwise, generally ignore the use of the
	   config argument.
	Raises:
	 - IOError if no file is found
	"""
	retcfg = default
	if config:
		retcfg = config
	return yaml.load(file(find_local_config_path(retcfg)))

class LocalConfigDefault(object):
	"""
	This is a caching singleton for the behavior of fetch_local_config
	"""
	def __init__(self):
		self.config_types = {}

	def __call__(self):
		return self

	def fetch_config(self, default, config=None):
		"""
		Returns the content of a yml config file as a hash.  If this config
		file has been read, 
		this will instead return a cached value.
		Parameters:
		 - default: default file name to look for
		 - config: override with this file name instead. (optional)
		   Note: the pattern of using config is intended to make using this with
		   OptionsParser easier.  otherwise, generally ignore the use of the
		   config argument.
		Raises:
		 - IOError if no file is found
		"""
		key = str(default) + "__" + str(config)
		if self.config_types.has_key(key):
			return self.config_types[key]
		else:
			value = fetch_local_config(default, config)
			self._add_config(value, default, config)
			return value

	def fetch_discovery(self, service_class, service_name):
		"""
		Call this function to fetch a discovery file from local config.
		Parameters:
		 - service_class: Class of the service
		 - service_name: Name of the service
		"""
		path = ['discovery', service_class, service_name]
		disc = self.fetch_config('/'.join(path))
		if not disc.has_key('server_list'):
			disc = {'server_list': [disc]}
		return disc

	def _add_config(self, config_hash, default, config=None):
		"""
		Adds a config to the cache
		"""
		key = str(default) + "__" + str(config)
		self.config_types[key] = config_hash

LocalConfig = LocalConfigDefault()

class LocalConfigTest(LocalConfigDefault):
	"""
	This is a caching singleton for testing.  Use this class
	to override the default behavior of LocalConfig like so:
	import pettingzoo.local_config as local_config
	cache = {'memcached/sessions.yml__None':
	  {'memcache':
	    {'namespace': 'test', 'port': 11211, 'address': 'localhost'}}}
	local_config.LocalConfig = local_config.LocalConfigTest(cache)
	"""
	def __init__(self, config_types={}):
		self.config_types = config_types

	def fetch_config(self, default, config=None):
		"""
		Returns values from the cache
		"""
		key = str(default) + "__" + str(config)
		return self.config_types[key]

	def add_config(self, config_hash, default, config=None):
		"""
		Adds a dict to the cache for tests
		"""
		self._add_config(config_hash, default, config)
