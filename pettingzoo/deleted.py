import zc.zk
import zookeeper
import threading
import sys
import traceback
import pettingzoo.testing

class Deleted(zc.zk.NodeInfo):
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
		self.key = (self.event_type, self.path)
		if self.session.watches.add(self.key, self):
			try:
				self._watch_key()
			except zookeeper.ConnectionLossException:
				watches = set(self.session.watches.pop(self.key))
				for w in watches:
					w._deleted()
				if self.session in watches:
					watches.remove(self.session)
				if watches:
					zc.zk.logger.critical('lost watches %s' % watches)
				raise
			pass
		else:
			self._notify(self.session.exists(self.path))

	def _watch_key(self):
		"""
		Internal function, not intended for external calling
		"""
		zkfunc = getattr(zookeeper, 'exists')
		def handler(h, t, state, p, reraise=False):
			if state != zookeeper.CONNECTED_STATE:
				zc.zk.logger.warning(
					"Node watcher event %r with non-connected state, %r",
					t, state)
				return
			try:
				assert h == self.session.handle
				assert state == zookeeper.CONNECTED_STATE
				assert p == self.path
				if self.key not in self.session.watches:
					return
				assert t == self.event_type or \
					t == self.event_type | pettingzoo.testing.TESTING_FLAG
				try:
					v = zkfunc(self.session.handle, self.path, handler)
				except zookeeper.NoNodeException:
					if t & pettingzoo.testing.TESTING_FLAG:
						v = None
						for watch in self.session.watches.watches(self.key):
							watch._notify(v)
					else:
						self._rewatch()
				else:
					for watch in self.session.watches.watches(self.key):
						watch._notify(v)
			except:
				exc_class, exc, tb = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tb)
				zc.zk.logger.exception(
					"%s(%s) handler failed", 'exists', self.path)
				if reraise:
					raise exc_class, exc, tb
		handler(
			self.session.handle,
			self.event_type,
			self.session.state,
			self.path,
			True)

	def _rewatch(self):
		"""
		Internal function, not intended for external calling
		"""
		#FIXME: this is not being exercised in tests
		for watch in self.session.watches.pop(self.key):
			try:
				self.path = self.session.resolve(self.path)
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
					if isinstance(v, zc.zk.CancelWatch):
						zc.zk.logger.debug(
							"cancelled watch(%r, %r)", self, callback)
					else:
						zc.zk.logger.exception("watch(%r, %r)", self, callback)

