import jsonpickle

__all__ = ['NumpyHandler']

class NumpyHandler(jsonpickle.handlers.BaseHandler):
	"""Abstract base class for handlers."""

	def flatten(self, obj, data):
		"""Flatten obj into a json-friendly form."""
		data['__numpy__'] = obj.tolist()
		return data

	def restore(self, obj):
		"""Restores the obj to type"""
		import numpy as np
		# return UnicodeMixin(obj['value'])
		return np.array(obj['__numpy__'])
	

