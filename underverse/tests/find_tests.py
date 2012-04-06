from underverse import Underverse
from underverse.model import Document
from underverse.predicates import Predicate as P
from test_data_gen import Person
import unittest, os

class Comment(object):
	"""docstring for Comment"""
	def __init__(self, text):
		super(Comment, self).__init__()
		self.text = text

class User(object):
	def __init__(self, name, fullname, password):
		self.name = name
		self.fullname = fullname
		self.password = password

class FindTestCase(unittest.TestCase):
	def setUp(self):
		self.uv = Underverse()
		self.uv.load('speed_test_smaller.sql')

	def tearDown(self):
		self.uv.close()

	def test_find_one(self):
		self.test = self.uv.test
		me = self.uv.test.find_one(Document.name == 'Max')
#		print me, type(me)
		self.assertTrue(me.name == 'Max')

	def test_eq(self):
		eq = True
		ppl = self.uv.test.find(Document.name == 'Max')

		for person in ppl:
			if person.name != 'Max':
				eq = False

		self.assertTrue(eq)

	def test_ne(self):
		test = True
		ppl = self.uv.test.find(Document.name != 'Max')

		for person in ppl:
			if person.name == 'Max':
				test = False

		self.assertTrue(test)

	def test_lt(self):
		test = True
		ppl = self.uv.test.find(Document.name < 27)

		for person in ppl:
			if person.name >= 27:
				test = False

		self.assertTrue(test)

	def test_lte(self):
		test = True
		ppl = self.uv.test.find(Document.name <= 27)

		for person in ppl:
			if person.name > 27:
				test = False

		self.assertTrue(test)

	def test_gt(self):
		test = True
		ppl = self.uv.test.find(Document.name > 27)

		for person in ppl:
			if person.name <= 27:
				test = False

		self.assertTrue(test)

	def test_gte(self):
		test = True
		ppl = self.uv.test.find(Document.name >= 27)

		for person in ppl:
			if person.name < 27:
				test = False

		self.assertTrue(test)

	def test_len(self):
		test = True
		ppl = self.uv.test.find(Document.name.len(3))
		
		for person in ppl:
			if len(person.name) != 3:
				test = False

		self.assertTrue(test)

	def test_btw(self):
		test = True
		ppl = self.uv.test.find(Document.age.btw(18, 25))

		for person in ppl:
			if person.age <= 18 and person.age <= 25:
				test = False

		self.assertTrue(test)

	def test_in(self):
		test = True
		ppl = self.uv.test.find(Document.age.in_([18, 25]))

		for person in ppl:
			if not person.age in [18, 25]:
				test = False

		self.assertTrue(test)

	def test_nin(self):
		test = True
		ppl = self.uv.test.find(Document.age.nin([18, 25]))

		for person in ppl:
			if person.age in [18, 25]:
				test = False

		self.assertTrue(test)

	def test_match(self):
		import random, re
		test = True
		ips = self.uv.ips
		for i in range(250):
			ips.add({'ip': '.'.join(str(random.randint(0, 255)) for i in range(4))})
		ppl = ips.find(Document.ip.match('^\d+\.\d+\.\d+\.\d+$'))

		for ip in ips:
			if not re.compile('^\d+\.\d+\.\d+\.\d+$').match(ip.ip):
				test = False

		self.assertTrue(test)

	def test_match2(self):
		import random, re
		test = True
		ips = self.uv.ips
		for i in range(250):
			ips.add({'ip': '.'.join(str(random.randint(0, 255)) for i in range(4))+'@8080'})
		ppl = ips.find(Document.ip.match('^\d+\.\d+\.\d+\.\d+$'))

		for ip in ips:
			if not re.compile('^\d+\.\d+\.\d+\.\d+$').match(ip.ip):
				test = False

		self.assertFalse(test)

	def test_search(self):
		import random, re
		test = True
		ips = self.uv.ips
		for i in range(250):
			ips.add({'ip': '.'.join(str(random.randint(0, 255)) for i in range(4))+'@8080'})
		ppl = ips.find(Document.ip.search('\d+\.\d+\.\d+\.\d+'))

		for ip in ips:
			if not re.compile('\d+\.\d+\.\d+\.\d+').search(ip.ip):
				test = False

		self.assertTrue(test)

	def test_orderby(self):
		test = True
		ppl = self.uv.test.find(Document.orderby('age'))
	
		age = -1
		for person in ppl:
			if person.age < age:
				test = False
			age = person.age
		
		self.assertTrue(test)

	def test_orderby2(self):
		test = True
		# print type(self.uv.test.find())
		ppl = self.uv.test.find(Document.orderby('-age'))
		age = 100
		for person in ppl:
			if person.age > age:
				test = False
			age = person.age
		
		self.assertTrue(test)

	def test_orderby3(self):
		test = True
		ppl = self.uv.test.find(Document.orderby('name','-age', 'college'))
		# print
		age = 100
		name = 'aa'
		for person in ppl:
			# print person.name, person.age, person.college
			if person.name < name:
				test = False
			if person.name != name:
				age = 100
			if person.age > age:
				test = False
			age = person.age

	def test_orderby4(self):
		test = True
		ppl = self.uv.test.find(Document.orderby())
		# print
		prev = ''
		for person in ppl:
			# print person.name, person.age, person.college
			if str(person) < prev:
				test = False
			prev = str(person)

	def test_orderby5(self):
		test = True
		ppl = self.uv.test.orderby('name')
		prev = ''
		for person in ppl:
			if person.name < prev:
				test = False
			prev = person.name
		self.assertTrue(test)

	def test_orderby6(self):
		test = True
		ppl = self.uv.test.orderby('name','-age', 'college')
		# print
		age = 100
		name = 'aa'
		for person in ppl:
			# print person.name, person.age, person.college
			if person.name < name:
				test = False
			if person.name != name:
				age = 100
			if person.age > age:
				test = False
			age = person.age

	def test_orderby7(self):
		test = True
		ppl = self.uv.test.all().orderby('name','-age', 'college')
		# print
		age = 100
		name = 'aa'
		for person in ppl:
			# print person.name, person.age, person.college
			if person.name < name:
				test = False
			if person.name != name:
				age = 100
			if person.age > age:
				test = False
			age = person.age

	def test_orderby8(self):
		test = True
		ppl = self.uv.test.find(Document.age > 25).orderby('name','-age', 'college')
		# print
		age = 100
		name = 'aa'
		for person in ppl:
			# print person.name, person.age, person.college
			if person.name < name:
				test = False
			if person.name != name:
				age = 100
			if person.age > age:
				test = False
			age = person.age

	def test_find_all(self):
		self.test = self.uv.test
		length = len(list(self.test))
		results = self.test.find()
		self.assertTrue(len(list(results)) == length)

	def test_find_call(self):
		self.test = self.uv.test
		length = len(list(self.test.find(Document.name == 'Max')))
		results = self.test(Document.name == 'Max')
		self.assertTrue(len(list(results)) == length)

	def test_find_limit(self):
		self.test = self.uv.test
		# length = len(list(self.test))
		results = self.test.find(Document.limit(5))
		self.assertTrue(len(list(results)) == 5)

	def test_find_skip(self):
		self.test = self.uv.test
		length = len(list(self.test))
		results = self.test.find(Document.skip(5))
		self.assertTrue(len(list(results)) == length - 5)

	def test_limit(self):
		self.test = self.uv.test
		# length = len(list(self.test))
		results = self.test.limit(5)
		self.assertTrue(len(list(results)) == 5)

	def test_skip(self):
		self.test = self.uv.test
		length = len(list(self.test))
		results = self.test.skip(5)
		self.assertTrue(len(list(results)) == length - 5)

	def test_limit_skip(self):
		self.test = self.uv.test
		results = self.test.limit(5).skip(2)
		self.assertTrue(len(list(results)) == 3)

	def test_skip_limit(self):
		self.test = self.uv.test
		results = self.test.skip(5).limit(2)
		self.assertTrue(len(list(results)) == 2)

	def test_get_attr(self):
		self.test = self.uv.test
		names = list(self.test.all().name)
		self.assertTrue(type(names) == list and len(names) == 250)

	def test_get_nested(self):
		self.test = self.uv.test2

		# self.uv.create_mappers(Comment, User)

		for i in range(5):
			ed_user = User('ed', 'Ed Jones', '3d5_p455w0r6')
			ed_user.comment = Comment('hey%s' % i)
			self.test.add(ed_user)

		u = self.test.find(Document.comment.text == 'hey1')
		# print list(u)
		self.assertTrue(u != None)
		for i in u:
			self.assertTrue(i.comment.text == 'hey1')

	def test_get_nested2(self):
		self.test2 = self.uv.test2

		# self.uv.create_mappers(Comment, User)

		for i in range(5):
			ed_user = User('ed', 'Ed Jones', '3d5_p455w0r6')
			ed_user.comment = Comment('hey%s' % i)
			self.test2.add(ed_user)

		from underverse.model import Document as D
		u = self.test2.find_one(Document.comment.text == 'hey1')
		# print Document.comment.text == 'hey1'
		# for t in self.test2:
			# print t
		self.assertTrue(u != None)
		self.assertTrue(u.comment.text == 'hey1')

	def test_and(self):
		test = self.uv.test
		from underverse.predicates import AND

		r = test.find(AND(Document.name == 'Max', Document.age < 25))
		for x in r:
			self.assertTrue(x.name == 'Max' and x.age < 25)

	def test_or(self):
		test = self.uv.test
		from underverse.predicates import OR

		r = test.find(OR(Document.name == 'Max', Document.name == 'Zaphod'))
		for x in r:
			self.assertIn(x.name, ['Max', 'Zaphod'])

	def test_complex_andor(self):
		test = self.uv.test
		from underverse.predicates import AND, OR

		# r = test.find(Document.name.in_(['Max', 'Zaphod']), Document.age < 35)
		# r = test.find(OR(Document.name == 'Max', Document.name == 'Zaphod'), Document.age < 35)
		r = test.find(OR(Document.age.btw(30, 35), Document.age.btw(60, 65)))
		for x in r:
			# print x
			self.assertTrue((x.age > 30 and x.age < 35) or (x.age > 60 and x.age < 65))

	def test_complex_andor2(self):
		test = self.uv.test
		from underverse.predicates import AND, OR

		# r = test.find(Document.name.in_(['Max', 'Zaphod']), Document.age < 35)
		# r = test.find(OR(Document.name == 'Max', Document.name == 'Zaphod'), Document.age < 35)

		# select * from test where ((age between 30 and 35) or (age between 60 and 65)) and (name = 'Billy' or name = 'Zaphod'));
		r = test.find(OR(Document.age.btw(30, 35), Document.age.btw(60, 65)), OR(Document.name == 'Billy', Document.name == 'Zaphod'))
		# r = test.find(OR(D.age.btw(30, 35), D.age.btw(60, 65)), OR(D.name == 'Billy', D.name == 'Zaphod'))
		for x in r:
#			print x
			self.assertTrue((x.age > 30 and x.age < 35) or (x.age > 60 and x.age < 65))
			self.assertIn(x.name, ['Billy', 'Zaphod'])

	def test_complex_andor3(self):
		test = self.uv.test
		from underverse.predicates import AND, OR

		# r = test.find(Document.name.in_(['Max', 'Zaphod']), Document.age < 35)
		# r = test.find(OR(Document.name == 'Max', Document.name == 'Zaphod'), Document.age < 35)
		
		# a = AND(Document.name == 'Zaphod', Document.age.btw(60, 65))
		# print a
	
		# a2 = AND(Document.name == 'Billy', Document.age == 31)
		# print a2

		# print OR(AND(Document.name == 'Zaphod', Document.age.btw(60, 65)), AND(Document.name == 'Billy', Document.age == 31))

		# select * from test where (name = 'Billy' and age = 31) or (name = 'Zaphod' and (age between 60 and 65)));
		r = test.find(OR(AND(Document.name == 'Zaphod', Document.age.btw(60, 65)), AND(Document.name == 'Billy', Document.age == 31)))
		# r = test.find(OR(D.age.btw(30, 35), D.age.btw(60, 65)), OR(D.name == 'Billy', D.name == 'Zaphod'))
		for x in r:
#			print x
			self.assertTrue((x.age > 30 and x.age < 35) or (x.age > 60 and x.age < 65))
			self.assertIn(x.name, ['Billy', 'Zaphod'])

	def test_paginate(self):
		test = self.uv.test_paging
		for i in range(15):
			test.add({'a': i, 'b': i*2, 'c': i*3})
		
		for page in test.paginate(5):
			# print page
			# print len(page)
			self.assertTrue(len(page) == 5)


if __name__ == '__main__':

	# suite = unittest.TestSuite()
	# suite.addTest(FindTestCase('test_paginate'))
	suite = unittest.TestLoader().loadTestsFromTestCase(FindTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)

	exit()

	import json, numpy as np, time, random
	from underverse import NecRow
	# import time
	# print type(np.arange(5))
	
	uv = Underverse()

	class AnotherTestClass(object):
		"""docstring for AnotherTestClass"""
		def __init__(self, text):
			super(AnotherTestClass, self).__init__()
			self.a = Comment(text)
			self.msg = Message(2,3,4)
			self.array = range(3)

		def _sum(self, value):
			return sum(self.array) + value

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
			self.n = np.arange(5)
			self.m = Comment('Starting to come together')
	
	msg = Message(1, 2, 3)
	print msg.m.len()

	print msg.__dict__

	# msg.update([{'x', 5}])
	# print msg.__dict__

	test = uv.testing
	# data = [Person().__dict__ for i in range(50)]

	# data = [Message(i, i*2, i**3) for i in range(3)]
	# test.add(data)

	def udp(doc):
		if '1' in doc.text:
			return True
		return False

	# data = [AnotherTestClass() for i in range(3)]
	data = []
	data.append(AnotherTestClass('test #1'))
	data.append(AnotherTestClass('test #2'))
	data.append(AnotherTestClass('test #3'))
	test.add(data)

	# result = test(Document.a.udp(udp))
	for r in test:
		print r.a.text
		print r._sum(5), r.array
		# print r

	uv.dump('testing.sql')
	print "Done."
	
	exit()
	# uv.load('speed_test_smaller.sql')

	# def name(array):
	# 	for row in array:
	# 		yield row.name, row

	# for name, count in uv.test.mapreduce(name, len, expand=True, sort=True):
	# 	print name, count

	# for person in sorted(uv.test.find(Document.name == 'Zaphod', Document.limit(15)), key=lambda x: x.friends):
		# print person
	
	print

	# for person in sorted(uv.test.find(Document.age.in_([35, 45, 55]), Document.limit(250)).all(), key=orderby('name', 'age', 'friends')):
		# print ''.join([str(attr).ljust(15) for attr in [person.name, person.age, person.friends, person.college]])
	# for person in uv.test.find(Document.name == 'Zaphod', Document.limit(50)):
		# print person

	# for person in uv.test.find(Document.age.in_([35, 45]), Document.limit(250), Document.orderby('name')):
		# print ''.join([str(attr).ljust(15) for attr in [person.name, person.age, person.friends, person.college]])

	# for person in uv.test.find(Document.age.in_([35, 45]), Document.limit(250)).orderby('name', 'age', 'friends'):
	# 	print ''.join([str(attr).ljust(15) for attr in [person.name, person.age, person.friends, person.college]])

	
#	me = uv.test.find_one(Document.name == 'Max', Document.limit(1))
#	print me, type(me)
#	print

	# for person in uv.test.find(Document.name == 'Max', Document.limskip(1, 1)):
	# 	print person
	
	# print
	
	# for person in uv.test.find(Document.name == 'Max', Document.skip(1)):
	# 	print person
	# print
	
	# for person in uv.test.find(Document.name == 'Max', Document.orderby('college')):
	# 	print person

	# encoders = []
	# decoders = []

	# encoders.append([lambda obj: True if isinstance(obj, np.ndarray) else False, lambda obj: {'__numpy__' : obj.tolist()}])
	# decoders.append([lambda obj: True if '__numpy__' in obj else False, lambda obj: np.array(obj['__numpy__'])])

	import json, numpy as np

	data = [Person().__dict__ for i in range(50000)]
	# attrs = 'name', 'age'

	# qd = QuasiDead.from_dicts(data, 'name', 'age')
	# print
	# print qd.find((qd.name == 'Max') | (qd.name == 'John'))
	# print qd.unique('name', 'age')

	import time
	# start = time.time()
	# gb = qd.groupby('name')
	# for g in gb:
	# 	pass
	# print "QD: ", time.time() - start
	
	test = uv.testing
	test.add(data)
	print len(test)
	
	start = time.time()
	qd = test.purify('name', 'age', 'gender')
	gb = qd.groupby('name')

	for name, ppl in gb:
		for age, ages in ppl.groupby('age'):
			# pass		# print name, len(ppl)
			print name, age, len(ages)
			# print name, age
		# pass
	print "UV - QD: ", time.time() - start

	print
	start = time.time()
	gb = test.groupby('name')
	for name, ppl in gb:
		for age, ages in ppl.groupby('age'):
			print name, age, len(ages)
			# pass
		# print name, len(ppl)
		# pass
	
	print "UV: ", time.time() - start
	
	# for person, ppl in qd.groupby('name'):
		# print person, len(ppl)


	# data = [{'x': 1.0, 'y': 1, 'z': range(5)},
	# 		{'x': 2.0, 'y': 2, 'z': range(5)},
	# 		{'x': 3.0, 'y': 3, 'z': range(5)},
	# 		{'x': 4.0, 'y': 4, 'z': range(5)},
	# 		{'x': 5.0, 'y': 5, 'z': range(5)}]

	# qd = QuasiDead.from_dicts(data, attrs)
	# print qd.dtype
	# print qd.__dict__

	# x = np.array([(1.0, 2, range(5)), (3.0, 4, range(5))], dtype=[('x', float), ('y', int), ('z', list)])
	# x = x.view(np.recarray)
	# print np.nonzero(rec.name == 'Max')
	# dct = json.loads(data, object_hook=as_numpy)
	# print dct['range'] > 50
