import zc.zk;
import zookeeper
import threading

def connect_to_zk(servers):
	"""
	Function used to connect to zookeeper for pettingzoo.multiprocessing.
	Parameters:
		server - list of zookeeper servers to connect to in the form of a comma
			seperated list of addresses:port'localhost:2181,10.5.2.1:2181'
	Returns:
		zc.zk.ZooKeeper connection
	"""
	return zc.zk.ZooKeeper(servers)

class Exists(zc.zk.NodeInfo):
	"""
	This class is implementing the zc.zk
	"""
	event_type = zookeeper.DELETED_EVENT

	def __init__(self, session, path, callbacks=[]):
		zc.zk.ZooKeeper._ZooKeeper__zkfuncs[zookeeper.DELETED_EVENT] = 'exists'
		self.session = session
		self.path = path
		self.callbacks = callbacks
		self._set_watch()

	def _set_watch(self):
		"""
		Internal function, not intended for external calling
		"""
		self.real_path = real_path = self.session.resolve(self.path)
		self.key = (self.event_type, real_path)
		if self.session.watches.add(self.key, self):
			try:
				self._watch_key()
			except zookeeper.ConnectionLossException:
				watches = set(self.session.watches.pop(key))
				for w in watches:
					w._deleted()
				if self.session in watches:
					watches.remove(self.session)
				if watches:
					zc.zk.logger.critical('lost watches %s', watches)
				raise
			pass
		else:
			self._notify(self.session.exists(session.handle, self.real_path))

	def _watch_key(self):
		"""
		Internal function, not intended for external calling
		"""
		zkfunc = getattr(zookeeper, 'exists')
		def handler(h, t, state, p, reraise=False):
			if state != zookeeper.CONNECTED_STATE:
				zc.zk.logger.warning("Node watcher event %r with non-connected state, %r", t, state)
				return
			try:
				assert h == self.session.handle
				assert state == zookeeper.CONNECTED_STATE
				assert p == self.real_path
				if self.key not in self.session.watches:
					return
				assert t == self.event_type
				try:
					v = zkfunc(self.session.handle, self.real_path, handler)
				except zookeeper.NoNodeException:
					self._rewatch()
				else:
					for watch in self.session.watches.watches(self.key):
						watch._notify(v)
			except:
				zc.zk.logger.exception("%s(%s) handler failed", 'exists', self.real_path)
				if reraise:
					raise
		handler(self.session.handle, self.event_type, self.session.state, self.real_path, True)

	def _rewatch(self):
		"""
		Internal function, not intended for external calling
		"""
		for watch in self.session.watches.pop(key):
			try:
				self.real_path = self.session.resolve(self.path)
			except (zookeeper.NoNodeException, zc.zk.LinkLoop):
				zc.zk.logger.exception("%s path went away", watch)
				watch._deleted()
			else:
				self._set_watch()

	def _notify(self, data):
		"""
		Internal function, not intended for external calling
		"""
		if data == None:
			for callback in list(self.callbacks):
				try:
					callback(self)
				except Exception, v:
					self.callbacks.remove(callback)
					if isinstance(v, CancelWatch):
						logger.debug("cancelled watch(%r, %r)", self, callback)
					else:
						logger.exception("watch(%r, %r)", self, callback)

