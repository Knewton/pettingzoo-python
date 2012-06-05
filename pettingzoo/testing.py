import zookeeper
from zc.zk.testing import Node, badpath
import zc.zk.testing

TESTING_FLAG = 32
def create(self, handle, path, data, acl, flags=0):
	with self.lock:
		self._check_handle(handle)
		base, name = path.rsplit('/', 1)
		if flags & zookeeper.SEQUENCE:
			if not hasattr(self, 'sequences'):
				self.sequences = {}
			cnt = self.sequences.setdefault(path, -1) + 1
			self.sequences[path] = cnt 
			name = "%s%010d" % (name, cnt)
			path = "%s%010d" % (path, cnt)
		if base.endswith('/'):
			raise zookeeper.BadArgumentsException('bad arguments')
		node = self._traverse(base)
		for p in node.acl:
			if not (p['perms'] & zookeeper.PERM_CREATE):
				raise zookeeper.NoAuthException('not authenticated')
		if name in node.children:
			raise zookeeper.NodeExistsException()
		node.children[name] = newnode = Node(data)
		newnode.acl = acl
		newnode.flags = flags
		node.children_changed(handle, zookeeper.CONNECTED_STATE, base)
		if flags & zookeeper.EPHEMERAL:
			self.sessions[handle].add(path)
		return path

def exists(self, handle, path, watch=None):
	# exists watch doesn't work yet
	if badpath(path):
		raise zookeeper.BadArgumentsException('bad argument')
	with self.lock:
		if watch:
			node = self._traverse(path)
			node.watchers += ((handle, watch), )
		self._check_handle(handle)
		try:
			self._traverse(path)
			return True
		except zookeeper.NoNodeException:
			return False

def deleted(self, handle, state, path):
	watchers = self.watchers
	self.watchers = ()
	for h, w in watchers:
		w(h, zookeeper.DELETED_EVENT | TESTING_FLAG, state, path)
	watchers = self.child_watchers
	self.child_watchers = ()
	for h, w in watchers:
		w(h, zookeeper.DELETED_EVENT | TESTING_FLAG, state, path)

