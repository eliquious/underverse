from underverse import Underverse
from underverse.model import Document
from test_data_gen import Person
from time import clock

uv = Underverse()
test = uv.test

def load(collection, num):
	test.purge()

	ppl = [Person().__dict__ for p in range(num)]

	start = clock()
	for p in ppl:
		# person = Person()
		# d = {'name': person.name, 'age': person.age, 'gender': person.gender, 'friends': person.friends, 'college': person.college}
		collection.add(p)
	return "%s - %0.5f elapsed" % (num, clock() - start)

def load_bulk(collection, num, buffer=150000):
	test.purge()
	data = []

	ppl = [Person().__dict__ for p in range(num)]
	start = clock()
	for p, d in enumerate(ppl):
		# person = Person()
		# d = {'name': person.name, 'age': person.age, 'gender': person.gender, 'friends': person.friends, 'college': person.college}
		data.append(d)
		if p > 1 and p % buffer == 0:
			collection.add(data)
			print "%s - %0.5f elapsed" % (p, clock() - start)
			data = []
	collection.add(data)
	return "%s - %0.5f elapsed" % (num, clock() - start)

def timing(col, num, fast_only=False, buffer=150000):
	if not fast_only:
		print "load:      ", load(col, num)
	print "load_bulk: ", load_bulk(col, num, buffer)
	print

# timing(test, 25)
timing(test, 250)
# timing(test, 2500)
# timing(test, 25000)
# timing(test, 250000, fast_only=True)
#timing(test, 2500000, fast_only=True)

uv.dump('speed_test_smaller.sql')

# print load_bulk(col, 2500000, 15000)


# Single Row Insert
#               25    250    2500
# MongoDB     0.003  0.031   0.332
# Underverse  0.005  0.052   0.521
# CounchDB    0.395  1.648  18.507

# Bulk Inserting
#               25    250    2500  25000  250000
# MongoDB     0.001  0.015  0.136  1.421  14.826
# Underverse  0.004  0.040  0.397  3.953  41.778
# CounchDB    0.061  0.063  0.699  7.889  76.396



