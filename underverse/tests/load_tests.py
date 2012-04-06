from underverse import Underverse
from underverse.model import Document
from test_data_gen import Person
import datetime
import unittest, os


class AnotherTestClass(object):
	"""docstring for AnotherTestClass"""
	def __init__(self, text):
		super(AnotherTestClass, self).__init__()
		self.a = Comment(text)
		self.msg = Message(2,3,4)
		self.array = range(3)

class Comment(object):
	"""docstring for Comment"""
	def __init__(self, text):
		super(Comment, self).__init__()
		self.text = text

	def len(self):
		return len(self.text)

class Message(object):
	"""docstring for Message"""
	def __init__(self, x, y, z, **kwargs):
		super(Message, self).__init__()
		self.x = x
		self.y = y
		self.z = z
		try:
			import numpy as np
			self.n = np.arange(5)
		except ImportError, e:
			self.n = range(5)
		
		self.m = Comment('Starting to come together')

class LoadTestCase(unittest.TestCase):
	def setUp(self):
		self.uv = Underverse()

	def tearDown(self):
		self.uv.close()

	def test_create_verse(self):
		# """creates a verse"""
		self.users = self.uv.users
		self.assertIn(self.users._name, self.uv)

	def test_load_dict(self):
		# """loads a single python dict"""
		self.users = self.uv.users

		user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
		self.users.add(user)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		# print u
		# print user
		# print (u.name == user['name'], u.fullname == user['fullname'], u.password == user['password'])
		# print (u['name'] == user['name'] and u['fullname'] == user['fullname'] and u['password'] == user['password'])
		
		self.assertTrue(u['name'] == user['name'] and u['fullname'] == user['fullname'] and u['password'] == user['password'])
		# exit()

	def test_load_dict_len(self):
		# """loads a single python dict"""
		self.users = self.uv.users

		user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
		self.users.add(user)
		self.assertTrue(len(self.users) == 1)

	def test_load_multiple_objs(self):
		# """loads several python class objects"""
		self.users = self.uv.users

		for person in range(50):
			self.users.add(Person())

		self.assertTrue(len(self.users) == 50)

	def test_bulk_load(self):
		# """bulk loads several python class objects"""
		self.users = self.uv.users
		self.users.add([Person() for i in range(50)])
		self.assertTrue(len(self.users) == 50)

	def test_dump(self):
		self.users = self.uv.users
		self.users.add([Person() for i in range(50)])
		self.uv.dump('test.sql')
		self.assertTrue(os.path.exists('test.sql'))

	def test_load(self):
		self.uv.load('test.sql')
		self.assertTrue(len(self.uv.users) == 50)

	# @unittest.skip("takes a while")
	def test_load_large(self):
		self.uv.load('speed_test_smaller.sql')
		self.assertTrue(len(self.uv.test) == 250)

	def test_update(self):
		self.users = self.uv.users
		user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
		self.users.add(user)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		u.age = 25
		self.users.update(u)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		self.assertTrue(hasattr(u, 'age') and u.age == 25)

	def test_update_list(self):
		self.users = self.uv.users
		user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
		self.users.add(user)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		u.friends = ['Michael', 'Luke', 'Amy']
		self.users.update(u)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		self.assertTrue(hasattr(u, 'friends') and type(u.friends) == list and len(u.friends) == 3)

	def test_update_np_array(self):
		try:
			import numpy as np
		except ImportError:
			self.skipTest("NumPy not installed")

		self.users = self.uv.users
		user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
		self.users.add(user)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		u.list = np.arange(5)
		self.users.update(u)

		u = self.users.find_one(Document.name == 'ed', Document.password == '3d5_p455w0r6', Document.fullname == 'Ed Jones')
		self.assertTrue(hasattr(u, 'list') and type(u.list) == np.ndarray)

	def test_add_json_ext(self):
		try:
			import numpy as np
		except ImportError:
			self.skipTest("NumPy not installed")

		test = True
		try:
			Underverse.add_json_ext(np.ndarray, lambda obj: obj.tolist(), lambda obj: np.array(obj))
		except:
			raise
			true = False
		self.assertTrue(test)


	def test_load_and_read_objects(self):
		try:
			import numpy as np
		except ImportError:
			self.skipTest("NumPy not installed")

		test = self.uv.test
		test.add(AnotherTestClass('test #1'))
		
		good = True

		from underverse import NecRow
		for r in test:
			if not (type(r) == AnotherTestClass or type(r) == NecRow):
				good = False
			if not (type(r.a) == Comment):
				good = False
			if not (type(r.msg.n) == np.ndarray):
				good = False
			if not (type(r.msg.m) == Comment):
				good = False
			if not (type(r.msg) == Message):
				good = False
		self.assertTrue(good)

	def test_dump_objects(self):
		try:
			import numpy as np
		except ImportError:
			self.skipTest("NumPy not installed")

		test = self.uv.test
		test.add(AnotherTestClass('test #1'))
		self.uv.dump('obj_testing.sql')
		self.assertTrue(True)

	def test_load_objects(self):
		try:
			import numpy as np
		except ImportError:
			self.skipTest("NumPy not installed")

		# Underverse.create_mappers(AnotherTestClass, Comment, Message)

		self.uv.load('obj_testing.sql')
		good = True

		from underverse import NecRow
		for r in self.uv.test:
			if not (type(r) == AnotherTestClass or type(r) == NecRow):
				good = False
			if not (type(r.a) == Comment):
				good = False
			if not (type(r.msg.n) == np.ndarray):
				good = False
			if not (type(r.msg.m) == Comment):
				good = False
			if not callable(r.msg.m.len):
				good = False
			if not (type(r.msg) == Message):
				good = False
		self.assertTrue(good)

	def test_load_example(self):
		table = self.uv.helion

		#you can either add one row at a time
		table.add({'a':1, 'b': 2})

		# or do bulk inserts
		array = [
		  {'a':1,'b':2,'c':3},
		  {'a':4,'b':5,'c':6},
		  {'a':7,'b':8,'c':9}]

		table.add(array)
		self.assertTrue(True)

	def test_load_add_column(self):
		table = self.uv.data

		# or do bulk inserts
		array = [
		  {'a':1,'b':2,'c':3},
		  {'a':4,'b':5,'c':6},
		  {'a':7,'b':8,'c':9}]

		table.add(array)
		table.add_column([1, 2, 3], 'd')
		self.assertTrue(len(list(table.d)) == 3)

	def test_load_array(self):
		table = self.uv.data

		# or do bulk inserts
		array = [[1,2,3],[4,5,6],[7,8,9]]
		table.from_array(array, names=['x', 'y', 'z'])
		self.assertTrue(len(list(table.x)) == 3)

	def test_load_datetime(self):
		table = self.uv.data

		# # if the object is a datetime.datetime, then it's converted to a Python list 
		# # and saved in a Python dict with '__datetime__' as it's key
		# datetime_encoder = (lambda obj: True if isinstance(obj, datetime.datetime) else False, 
		#           lambda obj: {'__datetime__' : datetime.datetime.strftime(obj, "%Y-%m-%d %H:%M:%S.%f")})

		# # when decoding, if the dict has a key '__datetime__', 
		# # then the list is converted to a datetime.datetime object
		# datetime_decoder = (lambda obj: True if '__datetime__' in obj else False, 
		#           lambda obj: datetime.datetime.strptime(obj['__datetime__'], "%Y-%m-%d %H:%M:%S.%f"))
		# Underverse.add_json_ext(datetime_encoder, datetime_decoder, '__datetime__')

		dt = datetime.datetime.now()
		# print datetime.datetime.strftime(dt, "%Y-%m-%d %H:%M:%S.%f")
		
		table.add({'datetime': dt})
		self.assertTrue(len(list(table.datetime)) == 1)
		
		d = table.find_one()
		# print d.datetime
		self.assertTrue(type(d.datetime) == datetime.datetime)


	def test_load_date(self):
		table = self.uv.data2

		# # if the object is a datetime.date, then it's converted to a Python list 
		# # and saved in a Python dict with '__date__' as it's key
		# date_encoder = (lambda obj: True if isinstance(obj, datetime.date) else False, 
		#           lambda obj: {'__date__' : datetime.datetime.strftime(obj, "%Y-%m-%d")})

		# # when decoding, if the dict has a key '__date__', 
		# # then the list is converted to a datetime.date object
		# date_decoder = (lambda obj: True if '__date__' in obj else False,
		#           lambda obj: datetime.datetime.strptime(obj['__date__'], "%Y-%m-%d").date())
		# Underverse.add_json_ext(date_encoder, date_decoder, '__date__')

		dt = datetime.datetime.now().date()
		# print datetime.datetime.strftime(dt, "%Y-%m-%d %H:%M:%S.%f")
		
		table.add({'date': dt})
		self.assertTrue(len(list(table.date)) == 1)
		
		d = table.find_one()
#		print d
#		print d.date, type(d.date)
		self.assertTrue(type(d.date) == datetime.date)


	def test_load_time(self):
		table = self.uv.data2

		# # if the object is a datetime.time, then it's converted to a Python list 
		# # and saved in a Python dict with '__time__' as it's key
		# time_encoder = (lambda obj: True if isinstance(obj, datetime.time) else False, 
		#           lambda obj: {'__time__' : obj.strftime("%H:%M:%S.%f")})

		# # when decoding, if the dict has a key '__time__', 
		# # then the list is converted to a datetime.time object
		# time_decoder = (lambda obj: True if '__time__' in obj else False,
		#           lambda obj: datetime.datetime.strptime(obj['__time__'], "%H:%M:%S.%f").time())
		# Underverse.add_json_ext(time_encoder, time_decoder, '__time__')

		dt = datetime.datetime.now().time()
		# print datetime.datetime.strftime(dt, "%Y-%m-%d %H:%M:%S.%f")
		
		table.add({'time': dt})
		self.assertTrue(len(list(table.time)) == 1)
		
		d = table.find_one()
#		print d
#		print d.time, type(d.time)
		self.assertTrue(type(d.time) == datetime.time)


if __name__ == '__main__':

	# class Comment(object):
	# 	"""docstring for Comment"""
	# 	def __init__(self, text):
	# 		super(Comment, self).__init__()
	# 		self.text = text

	# 	def len(self):
	# 		return len(self.text)

	# c = Comment('hey')
	# print type(c).__module__
	# print type(c)

	# print Comment.__module__
	# print Comment.__class__.__name__

	suite = unittest.TestLoader().loadTestsFromTestCase(LoadTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)

	# suite = unittest.TestSuite()
	# suite.addTest(LoadTestCase('test_update_list'))
	# unittest.TextTestRunner(verbosity=2).run(suite)
