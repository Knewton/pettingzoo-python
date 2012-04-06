from abc import ABCMeta, abstractmethod

class Candidate:
	__metaclass__ = ABCMeta

	@abstractmethod
	def on_elected(self):
		'''
		should handle anything that needs to be done
		once a candidate has been elected leader
		'''

	@abstractmethod
	def on_close(self):
		'''
		should handle graceful shutdown
		'''
