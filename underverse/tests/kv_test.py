from underverse import Underverse, NecRow
from underverse.model import Document as D
from test_data_gen import Person
import unittest, os

class KeyValueTestCase(unittest.TestCase):
	def setUp(self):
		self.uv = Underverse()

	def tearDown(self):
		self.uv.close()

	def test_add_key(self):
		# """creates a verse"""
		self.test = self.uv.test
		self.test.put('key', 5)
		test = True
		try:
			self.test.get('key')
		except Exception, e:
			test = False
		self.assertTrue(test)

	def test_add_key_fail(self):
		# """creates a verse"""
		self.test = self.uv.test
		test = True
		try:
			self.test.get('key2')
		except Exception, e:
			test = False
		self.assertFalse(test)

	def test_add_key_obj(self):
		# """creates a verse"""
		self.test = self.uv.test

		class kv(object):
			"""docstring for kv"""
			def __init__(self, arg):
				super(kv, self).__init__()
				self.arg = arg

		self.test.put('key', kv(5))
		test = True
		try:
			k = self.test.get('key')
			# print k, k.arg
		except Exception, e:
			test = False
		self.assertTrue(test)

	def test_add_key_list(self):
		# """creates a verse"""
		self.test = self.uv.test
		self.test.put('key', range(5))
		test = True
		try:
			k = self.test.get('key')
			if type(k.value) != list:
				test = False
			# print k.uuid
		except Exception, e:
			test = False
		self.assertTrue(test)

	def test_add_key_dict(self):
		# """creates a verse"""
		self.test = self.uv.test
		self.test.put('key', {'a':1, 'b':range(5)})
		test = True
		try:
			k = self.test.get('key')
			if type(k.value) != dict:
				test = False
			if type(k.b) != list:
				test = False
		except Exception, e:
			test = False
		self.assertTrue(test)


if __name__ == '__main__':

	suite = unittest.TestLoader().loadTestsFromTestCase(KeyValueTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)

	# d = D.comment.text
	# print d
	# print "Done."

	exit()

	uv = Underverse()
	options = uv.options

	times = uv.times

	from datetime import datetime

	times.add({'time':datetime.now()})

	for t in times:
		print t

	exit()

	# n = KeyValue('key', 5)
	# n = NecRow({'uuid':'key', 'value':5})
	# options.add(n)
	# options.put('key', 5)

	# for o in options:
	#   # print o
	#   o.value2 = o.value * 2
	#   options.update(o)

	# print options.get('key')
	# print options.get('key2')

	# uv.dump('kv_tests.sql')

	# class ENUM(object):
	# 	"""docstring for ENUM"""
	# 	def __init__(self, _dict):
	# 		super(ENUM, self).__init__()
	# 		self.__dict__.update(_dict)

	options.put('DEVMODES', {'TEST':0,'DEV':1,'PROD':2})

	# DEVMODES = options.get('DEVMODES')
	# print
	# print DEVMODES.value['TEST']

	# options.remove('DEVMODES')

	# options.put('DEVMODES', {'TEST':0,'DEV':1,'PROD':2})
	# options.put('DEVMODES', ENUM({'TEST':0,'DEV':1,'PROD':2}))
	DEVMODES = options.get('DEVMODES')
	print DEVMODES.PROD
	# print type(DEVMODES)

	# for option in options:
	# 	print option

	DEVMODES = options.get('DEVMODES')
	# print getattr(DEVMODES, 'TEST')

	DEVMODES = options.get('DEVMODES.DEV')
	print DEVMODES

	options.add({'TEST':0,'DEV':1,'PROD':2})
	for o in options:
		print o.TEST

	test = uv.test

	class User(NecRow):
	  def __init__(self, name, fullname, password):
	      self.name = name
	      self.fullname = fullname
	      self.password = password

	class Comment(object):
		"""docstring for Comment"""
		def __init__(self, text):
			super(Comment, self).__init__()
			self.text = text

	for i in range(5):
		ed_user = User('ed', 'Ed Jones', '3d5_p455w0r6')
		ed_user.comment = Comment('hey%s' % i)
		# test.put(ed_user.name, ed_user)
		test.add(ed_user)

	# print test.get('ed.comment.text')

	# print getattr(ed_user, 'comment.text')

	# print
	d = D.comment.text == 'hey1'
	print d
	# d._name = 'comment.text'
	for u in test(D.comment.text == 'hey1'):
		print "'%s'" % u#.comment.text

	# u = test.find_one(D.comment.text == 'hey1')
	# print u.comment.text

	# print d

