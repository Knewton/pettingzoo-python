# PettingZoo
A collection of sharded system recipes for [ZooKeeper][] written in Python.

[ZooKeeper]: https://github.com/apache/zookeeper

### Why Petting Zoo?
We need to be able to do stream processing of observations of student interactions with course material. This involves multiple models that have interdependent parameters. This requires:

* Sharding along different axes dependent upon the models
* Subscriptions between models for parameters
* Dynamic reconfiguration of the environment to deal with current load

How does this help us scale?

* Makes discovery of dependencies simple
* Adds to reliability of system by quickly removing dead resources
* Makes dynamic reconfiguration simple as additional resources become available

### Current State

* Relies on [zc.zk][] (will eventually be merged with the unified [Kazoo][] project)
* Documented, doc strings
* Tests (mock ZooKeeper)
* All recipes implemented in a Java version as well

[zc.zk]: https://github.com/python-zk/zc.zk
[Kazoo]: https://github.com/python-zk/kazoo

### Recipes

* [Distributed Discovery](#distributed-discovery)
* [Distributed Bag](#distributed-bag)
* [Leader Queue](#leader-queue)
* [Role Match](#role-match)

* * *

## Distributed Discovery
Allows services in a dynamic, distributed environment to be able to be quickly alerted of service address changes.

* Most service discovery recipes only contain host:port, Distributed Discovery can share arbitrary data as well (using yaml)
* Can handle load balancing through random selection of config
* Handles rebalancing on pool change

### Why Distributed Discovery?

* Makes discovery of dependencies simple
* Adds to reliability of system by quickly removing dead resources
* Makes dynamic reconfiguration simple as additional resources become available

### DD Example: Write

	from pettingzoo.discovery import write_distributed_config
	from pettingzoo.utils import connect_to_zk
	conn = connect_to_zk('localhost:2181')
	config = {
		'header': {
			'service_class': 'kestrel', 'metadata': {
				'protocol': 'memcached', 'version': 1.0
			}
		},
		'host': 'localhost',
		'port': 22133
	}
	write_distributed_config(conn, 'kestrel', 'platform', config)

### DD Example: Read

	from pettingzoo.discovery import DistributedMultiDiscovery
	from pettingzoo.utils import connect_to_zk
	conn = connect_to_zk('localhost:2181')
	dmd = DistributedMultiDiscovery(conn)
	conf = None
	def update_configs(path, updated_configs):
		conf.update(updated_configs)
	conf = dmd.load_config('kestrel', 'platform', callback=update_configs)

* * *

## Distributed Bag
A recipe for a distributed bag (dbag) that allows processes to share a collection.  Any participant can post or remove data, alerting all others.

* Used as a part of [Role Match](#role-match)
* Useful for any case where processes need to share configuration determined at runtime

![dbag](http://i.imgur.com/CJgip.png)

### Why Distributed Bag?

* Can quickly alert processes as to who is subscribing to them
* Reduces load by quickly yanking dead subscriptions
* Provides event based subscriptions, making implementation simpler

### Dbag Details

* Sequential items contain the actual data
* Can be ephemeral
* Clients set delete watch on discrete items
* Token is set to id of highest item
* Clients set a child watch on the "Tokens" node
* Can determine exact adds and deletes with a constant number of messages per delta

![Distributed Bag diagram](http://imgur.com/iqcTT.png)

### Dbag Example

	import yaml
	from pettingzoo.dbag import DistributedBag
	from pettingzoo.utils import connect_to_zk
	
	...
	conn = connect_to_zk('localhost:2181')
	dbag = DistributedBag(conn, '/some/data')
	docs = {}
	def acb(cb_dbag, cb_id):
		docs[cb_id] = cb_dbag.get(cb_id)
	def rcb(cb_dbag, cb_id):
		docs.remove(cb.id)
	
	dbag.add_listeners(add_callback=acb, remove_callback=rcb)
	add_id = dbag.add(yaml.load(document), ephemeral=True)
	docs[add_id] = document

* * *

## Leader Queue

Recipe is similar to Leader Election, but makes it easy to monitor your spare capacity.

* Used in [Role Match](#role-match)
* As services are ready to do work, they create an ephemeral, sequential node in the queue.
* Any member always knows if either they are in the queue or at the front
* Watch lets leader know when it is elected

### Why Leader Queue?

* Gives a convenient method of assigning work
* Makes monitoring current excess capacity easy

### LQ Details

* Candidates register with sequential, ephemeral nodes
* Candidate sets delete watch on predecessor
* Candidate is elected when it is the smallest node
* When elected, candidate takes over its new role
* When ready, candidate removes itself from the queue
* Only one candidate needs to call get_children upon any node exiting

![Leader Queue diagram](http://imgur.com/sikWd.png)

### LQ Example

	from pettingzoo.leader_queue import LeaderQueue, Candidate
	from pettingzoo.utils import connect_to_zk
	
	class SomeCandidate(Candidate):
		def on_elected(self):
			<do something sexy>
		
	conn = connect_to_zk('localhost:2181')
	leaderq = LeaderQueue(conn)
	leaderq.add_candidate(SomeCandidate())

* * *

## Role Match

Allows systems to expose needed, long lived jobs, and for services to take over those jobs until all are filled.

* Dbag used to expose jobs
* Leader queue used to hold applicants
* Records which jobs are presently held with ephemeral node
* Lets a new process take over if a worker dies
* We use it for sharding/segmentation to dynamically adjust the shards as needed due to load

### Why Role Match?

* Core of our ability to dynamically adjust shards
* Lets the controlling process adjust problem spaces and have those tasks become automatically filled
* Monitoring is easy to identify who is working on what, when

### RM Details

* Leader monitors for open jobs
* Job holder creates an ephemeral assignment
* Assignment id matches job id, indicating that it is claimed

![Role Match diagram](http://imgur.com/CmsHs.png)

* * *

## Future: Distributed Config

* Allows service config to be recorded and changed with a yaml config
* Every process that connects creates a child node of the appropriate service
* Any change in a child node's config overrides the overall service config for that process
* Any change of the parent or child fires a watch to let the process know that it's config has changed