"""
.. module:: pettingzoo.discovery
.. moduleauthor:: Devon Jones <devon@knewton.com>

:platform: Unix

:synopsis: Pettingzoo discovery implements a zookeeper protocol designed to \
enable easy discovery of networked services.

pettingzoo.discovery allows a service or a program monitoring a service to store
yaml configs containing the necessary information to connect and use that
service.  Using zookeeper ephemeral nodes, these configs will only stay
available while a service maintains its zookeeper connection, enabling these
configs to be pulled as easily as just killing the service that placed them into
pettingzoo.  Services using the discovery configs can provide callbacks that
will be called every time the configs for a specific service changes.  This
enables them to reconfigure their connections very quickly as services are added
or removed from the pool.

Discovery config files/dicts must match a specific set of keys.  Further, the
service_class in the header must match the service class the config is being
written to.  Example minimal config for a service class of 'test'::

  {
    'header': {
      'service_class': 'test'
    }
  }

Example fleshed out config for mysql::

  {
    'header': {
      'service_class': 'mysql',
      'metadata' {
        'service_name': 'testdb',
        'key': 'local_test_db',
      }
    },
    'host': 'localhost',
    'port': 3306,
    'username': 'testy',
    'password': 'mctestcase',
    'encoding': 'utf8'
  }

Note: Metadata in the header is intended to allow the system to put information
that could be useful for debugging.

Pettingzoo discovery comes with a program (discoverycache) that can cache all
discovery configs from zookeeper on the local system as yaml files stored under
/etc/pettingzoo/discovery.  Files will be stored as
<service_class>/<service_name>.yml, with an arbitrary depth based on any slashes
(/) used in the service class name.  Configs from all available service boxes
with the service class and name will be stored as one file with the format of::

  {'server_list': [<one config hash per available box>]}

which in yaml looks like::

  server_list:
    - header:
        service_class: test
      host: 10.1.1.1
    - header:
        service_class: test
      host: 10.1.1.2

These files can in turn be used by pettingzoo as a fallback mechanisim if
zookeeper becomes unavailable for some reason.  It is suggested that you
run discoverycache as a regular cron job so that your local file system can be
kept reasonably up to date.
"""

from __future__ import absolute_import
import zc.zk
import zookeeper
import yaml
import random
import pettingzoo.local_config
from pettingzoo.utils import get_logger

CONFIG_PATH = "/discovery"

def _get_local_ip(interface='eth0'):
	"""
	Local helper function that returns the local ip address for a
	given interface.
	"""
	from netifaces import interfaces, ifaddresses, AF_INET
	addresses = {}
	if interface in interfaces():
		addresses = [
			i['addr'] for i in ifaddresses(interface).setdefault(
				AF_INET, [{'addr': None}] )]
		addresses = [addr for addr in addresses if addr]
		if len(addresses) > 0:
			return addresses[0]
		else:
			raise Exception(
				"Interface %s does not have an ip address" % interface)
	else:
		raise Exception("Interface %s does not exist" % interface)

def _config_path_to_class_and_name(path):
	"""
	Local helper function that takes a pettingzoo config path and returns it in
	the service_class/service_name format expected in this module
	"""
	path = path.split(".")[0]
	parts = path.split("/")
	if len(parts) >= 2:
		service_class = parts[-2]
		service_name = parts[-1]
		return service_class, service_name
	else:
		raise Exception("Config path cannot be parsed: %s" % path)

def _znode_to_class_and_name(znode):
	"""
	Local helper function that takes a full znode path that returns it in the
	service_class/service_name format expected in this module
	"""
	znode = znode.split("/")
	znode.pop(0)
	return (znode[0], znode[1])

def _znode_path(service_class, service_name, key=None):
	"""
	Local helper function that creates the znode path using the
	service_class/service_name format that is utilized in this module
	"""
	znarr = [CONFIG_PATH, service_class, service_name]
	if key:
		znarr.append(key)
	return "/".join(znarr)

def _set_metadata(config, service_name, key=None):
	header = config.setdefault('header', {})
	metadata = header.setdefault('metadata', {})
	metadata['service_name'] = service_name
	if key != None:
		metadata['key'] = key
	return config

class DistributedDiscovery(object):
	"""
	DistributedDiscovery is a class that uses zookeeper to be able to manage the
	configs necessary for systems to interact with one another.  It has a
	fallback scheme that does the following:

	1. Try and find the config in zookeeper. If there is more the one config \
	   for that service, select one at random and return it.
	2. If there is no config in zookeeper, attempt to find one on the file \
	   system using the rules from pettingzoo config.
	3. Error

	If any config changes in zookeeper for that service, a passed in callback
	will be executed, selecting a new node at random from the current nodes,
	allowing the user to reconfigure.

	:param connection: a zc.zk.ZooKeeper connection

	**Note**

	Callbacks should be in the form of some_callback(path, config) where path
	will be passed in as the znode path to the service, and config is the
	config hash
	"""
	def __init__(self, connection):
		self.connection = connection
		self.connection.create_recursive(
			CONFIG_PATH, "", acl=zc.zk.OPEN_ACL_UNSAFE)
		self.cache = {}
		self.callbacks = {}
		self.children = {}

	def _get_config_from_cache(self, znode_path, callback=None):
		if callback:
			cset = self.callbacks.setdefault(znode_path, set())
			cset.add(callback)
		return self.cache.get(znode_path, None)

	def _store_config_in_cache(self, znode_path, config):
		self.cache[znode_path] = config
		
	def load_config(self, service_class, service_name, callback=None):
		"""
		Returns a config using the fallback scheme for the class to select
		a config at random from the available configs for a particular service.

		:param service_class: the classification of the service \
		  (e.g. mysql, memcached, etc)
		:param service_name: the cluster of the service \
		  (production, staging, etc)
		:param callback: callback function to call if the config for this \
		  service changes. (Optional)
		:rtype: *dict* The dict of the config in the standard pettingzoo \
		  format.
		"""
		path = _znode_path(service_class, service_name)
		cached = self._get_config_from_cache(path, callback)
		if cached:
			get_logger().info("DistributedConfig.load_config %s/%s (cached)" %
				(service_class, service_name))
			rconfig = _set_metadata(
				validate_config(cached[1], service_class),
				service_name, cached[0])
			get_logger().debug("%s" % (rconfig))
			return rconfig
		config = self._load_znodes(path)
		if config:
			get_logger().info(
				"DistributedConfig.load_config %s/%s (zookeeper)" %
					(service_class, service_name))
			rconfig = _set_metadata(
				validate_config(config[1], service_class),
				service_name, config[0])
			get_logger().debug("%s" % (rconfig))
			return rconfig
		config = self._load_file_config(service_class, service_name)
		get_logger().info("DistributedConfig.load_config %s/%s (file)" %
			(service_class, service_name))
		rconfig = _set_metadata(
			validate_config(config, service_class), service_name)
		get_logger().debug("%s" % (rconfig))
		return rconfig

	def get_service_classes(self):
		"""
		Returns a list of current service classes.

		:rtype: *list* a list of service classes as strings
		"""
		retarr = []
		if self.connection.exists(CONFIG_PATH):
			children = self.connection.children(CONFIG_PATH)
			for child in children:
				retarr.append(child)
		return retarr

	def get_service_names(self, service_class):
		"""
		Returns a list of current service names for a service class.

		:param service_class: the service class to return service names for
		:rtype: *list* a list of service classes as strings
		"""
		retarr = []
		path = '/'.join([CONFIG_PATH, service_class])
		if self.connection.exists(path):
			children = self.connection.children(path)
			for child in children:
				retarr.append(child)
		return retarr

	def count_nodes(self, service_class, service_name):
		"""
		Counts the number of nodes for a service class/name combination

		:param service_class: the service class
		:param service_name: the service name
		:rtype: *int* a count of nodes
		"""
		count = 0
		path = '/'.join([CONFIG_PATH, service_class, service_name])
		if self.connection.exists(path):
			children = self.connection.children(path)
			count = len(children)
		return count

	def _load_znodes(self, path, add_callback=True):
		get_logger().info("DistributedConfig._load_znodes: %s. Callback: %s" %
			(path, add_callback))
		if self.connection.exists(path):
			children = self.connection.children(path)
			if add_callback:
				children(self._child_callback)
				self.children[path] = children
			if len(children) > 0:
				selectee = random.choice([c for c in children])
				znode = path + "/" + selectee
				config = (selectee, yaml.load(self.connection.get(znode)[0]))
				self._store_config_in_cache(path, config)
				return config

	def load_config_via_path(self, path, callback=None):
		"""
		Returns a config using the fallback scheme for the DistributedDiscovery
		to select a config at random from the available configs for a particular
		service.

		:param path: a pettingzoo config style config path \
		  (memcached/sessions, databases/knewmena.yml, etc)
		:param callback: callback function to call if the config for this \
		  service changes. (Optional)
		:rtype: *dict* the config

		**Note**
		
		The passed in path will work with any path that is compatible with
		pettingzoo config.
		"""
		service_class, service_name = _config_path_to_class_and_name(path)
		return self.load_config(service_class, service_name, callback)

	def _load_file_config(self, service_class, service_name):
		path = '/'.join(['discovery', service_class, service_name])
		config = pettingzoo.local_config.LocalConfig().fetch_config(path)
		if config.has_key('server_list'):
			config_list = config['server_list']
			selectee = random.choice(config_list)
			return selectee
		else:
			return config

	def _child_callback(self, children):
		path = children.path
		service_class, service_name = _znode_to_class_and_name(path)
		config = self._load_znodes(path, add_callback=False)
		callbacks = self.callbacks.get(path, [])
		get_logger().info("DistributedConfig._child_callback: %s" % (path))
		for callback in callbacks:
			conf = None
			if config:
				conf = config[1]
			else:
				get_logger().warning(
					"DistributedConfig._child_callback: NO CONFIGS AVAILABLE")
			callback(path, conf)

class DistributedMultiDiscovery(DistributedDiscovery):
	"""
	DistributedMultiDiscovery is a class that uses zookeeper to be able to
	manage the configs necessary for systems to interact with one another.  It
	has a fallback scheme that does the following:

	1. Try and find the config in zookeeper.  Return all configs for that \
	   service.
	2. If there is no config in zookeeper, attempt to find one on the file \
	   system using the rules from pettingzoo config.
	3. Error

	If any config changes in zookeeper for that service, a passed in callback
	will be executed, returning all configs for the service, allowing the user
	to reconfigure.  Refer to DistributedConfig for the majority of this class's
	methods.

	:param connection: a zc.zk.ZooKeeper connection

	**Note**
	callbacks should be in the form of some_callback(path, config) where path
	will be passed in as the znode path to the service, and config is the
	config hash
	"""
	def load_config(self, service_class, service_name, callback=None):
		"""
		Returns a config using the fallback scheme for DistributedDiscovery to
		select a config at random from the available configs for a particular
		service.

		:param service_class: the classification of the service \
		  (e.g. mysql, memcached, etc)
		:param service_name: the cluster of the service \
		  (production, staging, etc)
		:param callback: callback function to call if the config for this \
		  service changes. (Optional)
		:rtype: *list*  The list contains config dicts in the pettingzoo \
		  config format.
		"""
		path = _znode_path(service_class, service_name)
		cached = self._get_config_from_cache(path, callback)
		if cached:
			get_logger().info(
				"DistributedMultiConfig.load_config: %s/%s (cached)" %
					(service_class, service_name))
			rconfig = [
				_set_metadata(
					validate_config(conf[1], service_class),
					service_name, conf[0])
						for conf in cached]
			get_logger().debug("%s" % (rconfig))
			return rconfig
		config = self._load_znodes(path)
		if config:
			get_logger().info(
				"DistributedMultiConfig.load_config: %s/%s (zookeeper)" %
					(service_class, service_name))
			rconfig = [
				_set_metadata(
					validate_config(conf[1], service_class),
					service_name, conf[0])
						for conf in config]
			get_logger().debug("%s" % (rconfig))
			return rconfig
		config = self._load_file_config(service_class, service_name)
		get_logger().info("DistributedMultiConfig.load_config: %s/%s (file)" %
			(service_class, service_name))
		rconfig = [
			_set_metadata(
				validate_config(c, service_class), service_name)
					for c in config]
		get_logger().debug("%s" % (rconfig))
		return rconfig

	def _load_znodes(self, path, add_callback=True):
		get_logger().info(
			"DistributedMultiConfig._load_znodes: %s. Callback: %s" %
				(path, add_callback))
		if self.connection.exists(path):
			children = self.connection.children(path)
			if add_callback:
				children(self._child_callback)
				self.children[path] = children
			if len(children) > 0:
				config = []
				for child in children:
					znodep = path + "/" + child
					znode = self.connection.get(znodep)
					single = yaml.load(znode[0])
					config.append((child, single))
				self._store_config_in_cache(path, config)
				return config

	def _load_file_config(self, service_class, service_name):
		path = '/'.join(['discovery', service_class, service_name])
		config = pettingzoo.local_config.LocalConfig().fetch_config(path)
		if config.has_key('server_list'):
			return config['server_list']
		else:
			return [config]

	def _child_callback(self, children):
		path = children.path
		get_logger().info("DistributedMultiConfig._child_callback: %s" % (path))
		config = self._load_znodes(path, add_callback=False)
		callbacks = self.callbacks.get(path, [])
		for callback in callbacks:
			config_list = []
			if config:
				config_list = [conf[1] for conf in config]
			else:
				get_logger().warning(
					"DistributedConfig._child_callback: NO CONFIGS AVAILABLE")
			callback(path, config_list)

def write_distributed_config(connection, service_class, service_name, config,
		key=None, interface='eth0', ephemeral=True):
	"""
	Writes a discovery config file out to zookeeper.

	:param connection: a zc.zk.ZooKeeper connection
	:param service_class: the classification of the service \
	  (e.g. mysql, memcached, etc)
	:param service_name: the cluster of the service \
	  (production, staging, etc)
	:param config: A dict containing discovery config values following the \
	  pettingzoo discovery file rules.
	:param key: (Optional) A unique string used to differentiate providers of \
	  a config inside pettingzoo.  If a key is not provided, pettingzoo will \
	  automatically use the ip address of the computer this function is being \
	  called from.  The Key is needed if you wish to explicitly remove a config.
	:param interface: (Default eth0) If falling back to pettingzoo to generate \
	  the key, you can set which networkign device to use to generate the \
	  ip address
	:param ephermeral: (Default True) Determines if this discovery config will \
	  be written to zookeeper as an ephemeral node or not.  Practically what \
	  this means is that if your zookeeper connection closes, zookeeper will \
	  automatically remove the config.  You generally want this to be True.
	:rtype: *str* the key
	"""
	if not key:
		key = _get_local_ip(interface)
	path = _znode_path(service_class, service_name)
	connection.create_recursive(path, "", acl=zc.zk.OPEN_ACL_UNSAFE)
	config = _set_metadata(
		validate_config(config, service_class),
		service_name,
		key)
	payload = yaml.dump(config)
	flags = 0
	if ephemeral:
		flags = zookeeper.EPHEMERAL
	znode = _znode_path(service_class, service_name, key)
	if connection.exists(znode):
		connection.delete(znode)
	connection.create(znode, payload, zc.zk.OPEN_ACL_UNSAFE, flags)
	get_logger().info("write_distributed_config: %s/%s/%s, Ephemeral: %s" %
		(service_class, service_name, key, ephemeral))
	get_logger().debug("%s" % (config))
	return key

def remove_stale_config(connection, service_class, service_name, key):
	"""
	This function manually removes a discovery config from zookeeeper.  This can
	be used either to remove non ephemeral discovery configs, when they are no
	longer valid, or to remove ephemeral discovery configs when you don't wish
	to close the zookeeper connection.  For example when your configs are being
	written by a seperate monitoring process.

	:param connection: a zc.zk.ZooKeeper connection
	:param service_class: the classification of the service \
	  (e.g. mysql, memcached, etc)
	:param service_name: the cluster of the service \
	  (production, staging, etc)
	:param key: the key the specific discovery config was written out as.
	"""
	get_logger().info("remove_stale_config: %s/%s/%s" %
		(service_class, service_name, key))
	connection.delete(_znode_path(service_class, service_name, key))

def validate_config(config, service_class):
	"""
	Validates that a discovery config is valid for a specific service class
	"""
	header = config.setdefault('header', {})
	if service_class != header.get('service_class'):
		raise Exception(
			"Cannot store config hash of type %s in service class %s: %s" %
				(header.get('service_class'), service_class, config))
	return config

