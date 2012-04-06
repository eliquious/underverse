from predicates import Predicate as P

__all__ = ['DocumentModel', 'Document', 'QuasiDead', 'Join']

class DocumentModel(type):
	# """Base class for Model"""

	def __getattr__(self, attr):
		return Document(attr)

class Document(object):
	"""
This class is the main class for handling document queries. 

Here are some query examples::

	print Document.age < 5
	print Document.age <= 5
	print Document.age > 5
	print Document.age >= 5
	print Document.age == 5
	print Document.age != 5
	print Document.name.len(5)
	print Document.age.btw(20, 50)
	print Document.age
	print Document.name.type(str)
	print Document.name.in_(['Max'])
	print Document.name.nin(['Max'])
	print Document.name.match('Max')
	print Document.name.search('ax')
	print Document.name.nmatch('Max')
	print Document.name.nsearch('Max')

	# cheezy example
	def email_search(value, domain=None):
		if domain in value:
			return True
		else:
			return False

	#defining you own predicate functions...
	# if the function returns 'True', then the object will be included in the results, otherwise, it will be left out.
	#also, any arguments passed to the 'udp' function, they will be passed to the given predicate as input. 
	for d in verse.find(Document.email.udp(email_search, domain='gmail')):
		print d

	# for the above example, you could have done it like this.
	for d in verse.find(Document.email.search('gmail')):
		print d

To query for the first object, use the ``find_one`` function instead of ``find``.::

	# to find the first document, call find_one instead of find
	for d in verse.find_one(Document.email.udp(email_search, domain='gmail')):
		print d

As of v0.3.5, compound or nested query objects are supported. This means that you can do 
``Document.comment.text``, where comment is an object which has a ``text`` attribute.
Which basically means that querys can written to find documents where sub-attributes match a certain condition.

Here's an example:

.. code-block:: python

	uv = Undeverse()
	test = uv.test

	# create user class
	class User(NecRow):
	  def __init__(self, name, password):
	      self.name = name
	      self.password = password

	# create comment class
	class Comment(object):
		def __init__(self, text):
			super(Comment, self).__init__()
			self.text = text

	# create 5 users
	for i in range(5):
		user = User('user%s' % i, 'password')
		user.comment = Comment('hey%s' % i)
		test.add(user)

	# find all users that have posted a comment that equals 'hey1'
	# notice the possibility to stacking attributes for a more powerful query
	# in previous versions you would have had to write a UDP or UDF
	for u in test(D.comment.text == 'hey1'):
		print "'%s'" % u#.comment.text

	# find_one also works
	u = test.find_one(D.comment.text == 'hey1')
	print u.comment.text

	"""
	__metaclass__ = DocumentModel

	def __init__(self, name):
		super(Document, self).__init__()
		self._name = name
		self._predicate = P.exists(name)
		self._desc = "has key: "+name

	def __lt__(self, value):
		self._desc = "%s < %s" % (self._name, value)
		self._predicate = P.lt(self._name, value)
		return self

	def __le__(self, value):
		self._desc = "%s <= %s" % (self._name, value)
		self._predicate = P.lte(self._name, value)
		return self

	def __gt__(self, value):
		self._desc = "%s > %s" % (self._name, value)
		self._predicate = P.gt(self._name, value)
		return self

	def __ge__(self, value):
		self._desc = "%s >= %s" % (self._name, value)
		self._predicate = P.gte(self._name, value)
		return self

	def __eq__(self, value):
		self._desc = "%s == %s" % (self._name, value)
		self._predicate = P.eq(self._name, value)
		return self

	def __ne__(self, value):
		self._desc = "%s != %s" % (self._name, value)
		self._predicate = P.ne(self._name, value)
		return self

	def len(self, value):
		"""
Find documents where an attributes is a certain length

.. code-block:: python

	from underverse.model import Document as D

	# finds all users whose names are only 3 characters long
	uv.users.find(D.name.len(3))

		"""
		self._desc = "len(%s) == %s" % (self._name, value)
		self._predicate = P.len(self._name, value)
		return self

	def btw(self, left, right):
		"""
Finds documents where an attribute is between the given left and right inputs

.. code-block:: python

	from underverse.model import Document as D

	# finds all users whose age is between 18 and 25
	uv.users.find(D.age.btw(18, 25))

		"""
		self._desc = "%s < %s < %s" % (left, self._name, right)
		self._predicate = P.btw(self._name, left, right)
		return self

	def udp(self, function, *args, **kwargs):
		"""

User defined predicates or UDP can be used if the existing comparison operators are not enough.

.. code-block:: python

	from underverse.model import Document as D

	#filters out documents where the sqrt of a selected attribute is between a given range
	def sqrt_filter(doc, lower_bound=2, upper_bound=39):
		if upper_bound >= math.sqrt(doc) >= lower_bound:
			return True
		else:
			return False

	# finds all docs whose 'some_number' attribute's sqrt is between 16 and 64
	uv.docs.find(D.some_number.udp(sqrt_filter, lower_bound=16, upper_bound=64))


The UDP function only receives the 'some_number' attribute and has to return a bool discerning to filter out or include each row. 
You'll also notice that any ``*args`` or ``**kwargs`` are forwarded to the method. This is to allow for more flexibility and DRY code.

.. note::
	
	If you are astute, you'll quickly see the limitation of this method. However, it might be useful for some. 
	Because the ``udp`` method only passes one attribute, more complex filters will not work easily. 
	If you need a filter which requires multiple attributes of the data, use the ``udf`` method instead.

		"""
		self._desc = "%s(%s" % (function.__name__, self._name)
		if len(args) > 0:
			self._desc += ", " + ', '.join(args)
		if len(kwargs) > 0:
			self._desc += ", " + ', '.join(['%s=%s' % (k, v) for k, v in kwargs.items()])
		self._desc += ")"
		self._predicate = P.udp(self._name, function, *args, **kwargs)
		return self

	def type(self, value):
		"""
Finds documents where an attribute's type matches the input type

.. code-block:: python

	uv.users.find(D.age.type(int))
	
.. note::
	
	This may not ever be used...

		"""
		self._desc = "type(%s) == %s" % (self._name, value.__name__)
		self._predicate = P.type_(self._name, value)
		return self

	def in_(self, value):
		"""
Finds documents where an attribute is in the given list

.. code-block:: python

	from underverse.model import Document as D

	# finds all users whose name is either 'Max' or 'Tamara'
	uv.users.find(D.name.in_(['Max', 'Tamara']))

		"""
		self._desc = "%s in %s" % (self._name, value)
		self._predicate = P.in_(self._name, value)
		return self

	def nin(self, value):
		"""
Finds all documents where the attribute is NOT in the given list. Look at ``in_`` for an example.
		"""
		self._desc = "%s not in %s" % (self._name, value)
		self._predicate = P.nin(self._name, value)
		return self

	def in_set(self, value):
		"""
Finds documents where an attribute is in the given **set**.

.. note::

	This predicate calls *set* on the input before it is compared. This can be useful for passing other generators to the predicate. 
	However, if the input value is already a set, just call ``D.in_`` instead.

.. code-block:: python

	from underverse.model import Document as D

	# finds all users whose name is either 'Max' or 'Tamara'
	uv.users.find(D.name.in_(['Max', 'Tamara']))

		"""
		_set = set(value)
		self._desc = "%s in set(%s)" % (self._name, _set)
		self._predicate = P.in_(self._name, _set)
		return self

	def nin_set(self, value):
		"""
Finds all documents where the attribute is NOT in the given **set**. Look at ``in_set`` for an explanation.
		"""
		_set = set(value)
		self._desc = "%s not in set(%s)" % (self._name, _set)
		self._predicate = P.nin(self._name, _set)
		return self

	def in_list(self, value):
		"""
Finds documents where an attribute is in the given **list**

.. note::

	This predicate calls *list* on the input before it is compared. This can be useful for passing other generators to the predicate. 
	However, if the input value is already a list, just call ``D.in_`` instead.

.. code-block:: python

	from underverse.model import Document as D

	# finds all users whose name is either 'Max' or 'Tamara'
	uv.users.find(D.name.in_list(['Max', 'Tamara']))

		"""
		_set = list(value)
		self._desc = "%s in set(%s)" % (self._name, _set)
		self._predicate = P.in_(self._name, _set)
		return self

	def nin_list(self, value):
		"""
Finds all documents where the attribute is NOT in the given **list**. Look at ``in_list`` for an explanation.
		"""
		_set = list(value)
		self._desc = "%s not in set(%s)" % (self._name, _set)
		self._predicate = P.nin(self._name, _set)
		return self

	def match(self, value):
		"""
Uses the ``re`` module to match data attributes. The regex must match exactly. The given regex would fail if there was an attached port to the IP attribute.

.. code-block:: python

	# re.compile('10\.2\.1\.\d+').match(doc[ip])
	uv.docs.find(D.ip.match('10\.2\.1\.\d+'))
		
		"""
		self._desc = "re.compile('%s').match(%s)" % (value, self._name)
		self._predicate = P.match(self._name, value)
		return self

	def search(self, value):
		"""
Uses the ``re`` module to search data attributes. The regex doesn't have to match exactly. The given regex would NOT fail if there was an attached port to the IP attribute.

.. code-block:: python

	# re.compile('10\.2\.1\.\d+').search(doc[ip])
	uv.docs.find(D.ip.search('10\.2\.1\.\d+'))

		"""
		self._desc = "re.compile('%s').search(%s)" % (value, self._name)
		self._predicate = P.search(self._name, value)
		return self

	def nmatch(self, value):
		"""
This finds the opposite of the ``match`` predicate.
		"""
		self._desc = "not re.compile('%s').match(%s)" % (value, self._name)
		self._predicate = P.nmatch(self._name, value)
		return self

	def nsearch(self, value):
		"""
This finds the opposite of the ``search`` predicate.
		"""
		self._desc = "not re.compile('%s').search(%s)" % (value, self._name)
		self._predicate = P.nsearch(self._name, value)
		return self

	@staticmethod
	def orderby(*args):
		"""
Orders documents by one or more columns.

.. code-block:: python

	uv.docs.find(D.orderby('name', '-age'))
	
The ``orderby`` functionality now has *ASC* and *DESC* 
capability. Descending order is achieved by pre-pending 
a **-** (negative sign or hyphen) to the column name.

Notice that the *orderby* is actually inside the *find* function.

However, you can also do this:
	
.. code-block:: python

	uv.docs.orderby('name', '-age')

		"""
		if len(args) > 0:
			return P.orderby(*args)
		else:
			return lambda x: sorted(x)

	@classmethod
	def __lim__(self, array, count=2**32):
		c = 0
		docs = []
		for doc in array:
			if c < count:
				docs.append(doc)
			c += 1
		return docs

	@staticmethod
	def limit(value):
		"""
Limits the amount of documents found.

.. code-block:: python

	uv.docs.find(D.limit(50))

		"""
		if not type(value) is int:
			raise ValueError, "Limit predicates must be integers: limit(5)"

		return P.udf(Document.__lim__, count=value)

	@classmethod
	def __limskip__(self, array, _skip=0, _limit=2**32):
		c = 0
		l = 0
		docs = []
		for doc in array:
			if c >= _skip and l < _limit:
				docs.append(doc)
				l += 1
			c += 1
		return docs

	@staticmethod
	def limskip(_skip, _limit):
		"""
Skips the first few documents found and also limits the records found based on the given input.

.. code-block:: python

	# skips 50 records and returns the next 50
	uv.docs.find(D.limskip(50, 50))

		"""
		if not type(_skip) is int:
			raise ValueError, "Skip argument must be an integer: K.limskip(2, 5)"
		if not type(_limit) is int:
			raise ValueError, "Limit argument must be an integer: K.limskip(2, 5)"

		return P.udf(Document.__limskip__, _skip, _limit)

	@classmethod
	def __skip__(self, array, _skip=0):
		c = 0
		docs = []
		for doc in array:
			if c >= _skip:
				docs.append(doc)
			c += 1
		return docs

	@staticmethod
	def skip(value):
		"""
Skips the first few documents found based on the given input.

.. code-block:: python

	# skips 50 records
	uv.docs.find(D.skip(50))

		"""
		if not type(value) is int:
			raise ValueError, "Skip predicates must be integers: K.skip(5)"

		return P.udf(Document.__skip__, value)

	@staticmethod
	def udf(function, *args, **kwargs):
		"""
Passes the entire data stream to the user defined function along with 
any *args* and *kwargs*. This can be used to filter documents on 
multiple attributes of the data along with other advanced functionality.

.. note::
	
	The UDF takes the entire collection and returns a subset of documents 
	matching complex criterion. This differs from the UDP functionality 
	in the the UDP only receives a single attribute of one document at a time.

.. code-block:: python

	def complex_filter(array):
		subset = []
		for doc in array:
			if some_ninja_math:
				subset.append(doc)
		return subset

	uv.docs.find(D.udf(complex_filter))

Or a real example...

.. code-block:: python

	# finds all docs where x**y > 4
	def sq_filter(array, goal=2):
		subset = []
		for doc in array:
			if doc.x ** doc.y > goal:
				subset.append(doc)
		return subset

	for d in verse.find(Document.udf(sq_filter, 4)):
		print d

		"""
		return P.udf(function, *args, **kwargs)

	def __exists__(self):
		return P.exists(self._name)

	def __predicate__(self):
		return self._predicate

	@property
	def exists(self):
		return self.__exists__()

	@property
	def predicate(self):
		return self.__predicate__()

	def __str__(self):
		return "<Document: \"%s\">" % self._desc

	def __getattr__(self, attr):
		if attr in self.__dict__:
			return self.__dict__[attr]
		# print "doc"

		self._name = self.__dict__["_name"] + "." + attr
		self._predicate = P.exists(self._name)
		self._desc = "has key: " + self._name

		# setattr(self, "_name", )
		return self


class QuasiDead(object):
	"""
The QuasiDead are quoted to be those 'who deprive 
themselves of all nourishment ... for mental advancement'.

Think of the unstructured aspect of your data that is being 
forfeited to gain added functionality from NumPy. More specifically, the
QuasiDeads are user-defined subsets of the data contained 
in a document collection.

This data is turned into a NumPy recarray for perhaps more convenient 
data grouping, filtering and manipulation.

.. warning::
	
	Because of the heavy reliance upon NumPy, this added functionality 
	requires NumPy to be installed.

	"""
	def __init__(self, recarray):
		super(QuasiDead, self).__init__()
		self.recarray = recarray

	@staticmethod
	def __numpy_test__():
		try:
			import numpy as np
		except ImportError:
			raise ImportError, "NumPy must be installed to use this functionality."

	def find(self, *bools):
		"""
This function selects all documents where the logical AND of the arguments are true. 

.. note::

	This functionality requires NumPy to be installed as it uses NumPy as the back end.

.. code-block:: python

	uv = Underverse()
	uv.load('data.sql')
	
	users = uv.users

	# you must define data attributes to use in the NumPy recarray
	qd = users.purify('name', 'age', 'gender')
	
	# finds all males
	males = qd.find(qd.gender == 'M')
	
	#finds all males who are less than or equal to 25 years of age
	# multiple AND arguments are separated by commas
	young_males = qd.find(qd.gender == 'M', qd.age <= 25)
	
	# finds all users whose names are either 'Max' OR 'Tamara'
	# notice that the OR syntax is slightly different
	# OR conditions MUST be surrounded by parentheses and separated by a |
	# instead of being comma delimited
	young_males = qd.find((qd.name == 'Max') | (qd.name == 'Tamara'))
	

		"""
		QuasiDead.__numpy_test__()
		import numpy as np
		if len(bools) < 1:
			raise Exception, "Find must have at least one argument"
		resultant = bools[0]
		if len(bools) > 1:
			for _bool in bools[1:]:
				resultant *= _bool
		return self.__class__(self.recarray[np.nonzero(resultant)[0]])

	@staticmethod
	def from_array(array, *names):
		QuasiDead.__numpy_test__()
		import numpy as np
		return QuasiDead(np.core.records.fromrecords(array, names=','.join([n for n in names])))

	@staticmethod
	def from_dicts(dicts, *types):
		nu = []
		for dct in dicts:
			has_all = True
			tmp = []
			for attr in types:
				if not attr in dct:
					has_all = False
					break
				tmp.append(dct[attr])
			if has_all:
				nu.append(tuple(tmp))
		return QuasiDead.from_array(nu, *types)

	def __getattr__(self, attr):
		return getattr(self.recarray, attr)

	def __str__(self):
		return str(self.recarray)

	def __repr__(self):
		return repr(self.recarray)

	def __iter__(self):
		return iter(self.recarray)

	def __len__(self):
		return len(self.recarray)

	def __expandmap(self, mapped_results):
		'''
		This method expands a dictionary, where the keys are iterables, 
			into a list with the mapped values as the last index.
		'''
		QuasiDead.__numpy_test__()
		
		expanded_results = []
		for k, v in mapped_results.items():
			tmp = []
			if hasattr(k, '__iter__'):
				tmp.extend(k)
			else:
				if len(k) > 0 and type(k) != str and type(k) != np.string_:
					for l in k: tmp.append(l)
				else:
					tmp.append(k)
			tmp.append(v)
			expanded_results.append(tmp)
		return expanded_results

	def unique(self, *cols):
		"""
		Finds all the unique combinations of one of more columns.

		.. code-block:: python

			uv = Underverse()
			uv.load('data.sql')

			data = uv.data
			qd = data.purify('name', 'age')
			qd.unique('name', 'age')

		"""
		QuasiDead.__numpy_test__()
		import numpy as np
		base = np.core.records.fromarrays([getattr(self.recarray, c) for c in cols])
		return np.unique(base)

	def groupby(self, *cols):
		"""
Groups a QuasiDead instance by one or more attributes. This works *exactly* the same way as a Verse groupby.

.. code-block:: python

	uv = Underverse()
	uv.load('data.sql')

	qd = test.purify('name', 'age', 'gender')
	start = time.time()
	for name, ppl in qd.groupby('name'):
		print name, len(ppl)

		"""
		QuasiDead.__numpy_test__()
		from underverse.ordereddict import OrderedDict
		data = OrderedDict()
		for u in self.unique(*cols):
			tmp = []
			for i, c in enumerate(u):
				data[tuple(u)] = self.find(getattr(self.recarray, cols[i]) == c)
		return self.__expandmap(data)

	def orderby(self, *args):
		"""
Orders a QuasiDead instance by one or more attributes

.. code-block:: python

	qd.orderby('name'):

		"""
		QuasiDead.__numpy_test__()
		self.recarray.sort(order=args)
		return self


class Join(object):
	"""
This class facilitates 'join' operations in Underverse. Traditionally, join operations are used to
merge different datasets based on some common attribute. Underverse provides this functionality, 
as well as, methods for performing MUCH more powerful merging.

There are currently 3 different options for joining different datasets: *ON*, *UDP* and *UDF*.

* **ON**

	The *ON* functionality is designed to work similar to the *JOIN* in traditional SQL. 
	However, unlike SQL, all matching documents are stored in the result for every original document.

	.. code-block:: python

		# SELECT * from posts p JOIN comments c ON p.id = c.post_id
		for post in posts.join(comments, 'comments').on(D.id, D.post_id):

			# prints all comments found in the join operation
			print post.comments

	In the example above, all results are stored in the ``comments`` variable.

* **UDP**

	User-Defined Predicates are similar to the UDPs found in the *find* functionality. This method 
	allows coders to customize the *ON* functionality as explained above.
	UDPs **must** return either True or False.

	These predicates are slower than both of the other two join methods. This is because *ON* is O(2N) 
	while UDPs are O(N^2). UDPs are only slightly slower than UDFs. UDPs can provide complex join operations 
	with very little code.

	.. code-block:: python

		# This join finds all comments for each post that were created within the first 5 minutes of the post's creation
		for post in posts.join(comments, 'early_comments').udp(D.id, D.post_id, 
			lambda left, right: True if right.created_at - left.created_at < 300 else False):

			# prints all comments found in the join operation
			print post.early_comments

	You can also do this.

	.. code-block:: python

		def udp_(left, right):
			if r.created_at - left.created_at < 300::
				return True
			return False

		# This join finds all related comments AND only includes early comments within 5 minutes of the creation of the post
		for post in posts.join(comments, 'early_comments').udp(D.id, D.post_id, udp_):

			# prints all comments found in the join operation
			print post.early_comments


 	The code above finds all comments posted with 5 minutes of the post's creation. UDPs act similar to the *ON* 
 	clause in that it uses two attributes to match with. However, they give some freedom to filter on more than the two original attributes.
 	
 	.. note::
 		
 		**Traditional SQL**

 		UDPs can be compared to SQL like so:

 		Imagine this query, ``SELECT * FROM posts p JOIN comments c ON p.id = c.post_id WHERE c.created_at - p.created_at < 300;``.

 		The SQL above performs the same action as the code posted in the section above. 

	In the example above, all results are stored in the ``early_comments`` variable.

* **UDF**

	This is by far the most powerful join operation in Underverse. UDFs do not have the *ON* 
	functionality of UDPs. Therefore you can customize the JOIN logic 'til Underverse come..

	Join UDFs give the coder full reign over the algorithm used to find the documents you are looking for.
	Therefore you can use binary searches and the like to perform joining much faster than a simple loop.

	.. code-block:: python

		def early_comments(left, right_array):
			values = []
			for r in right_array:
				if r.post_id == l.id and r.created_at - left.created_at < 300:
					values.append(r)
			return values

		# This join finds all related comments AND only includes early comments within 5 minutes of the creation of the post
		for post in posts.join(comments, 'early_comments').udf(early_comments):

			# prints all comments found in the join operation
			print post.early_comments

.. admonition:: Performance Hint
  :class: perf


	*ON* joins are probably going to be the fastest in most if not all cases. However, that is 
	the least flexible method. **UDPs** are normally slower than UDFs just by the shear number of function calls.
	**UDFs** are the most flexible in terms of customization, and they allow the join operation to be tailored to the data. 
	Which means special algorithms can be used to greatly increase it's speed.

	"""
	def __init__(self, left, right, alias):
		super(Join, self).__init__()
		self.left = left
		self.right = right
		self.alias = alias

	def udp(self, left, right, udp):
		"""
User-Defined Predicates provide for slightly more flexible joins than the *ON* method.

.. seealso::

	Look at the class description for usage

		"""
		for l in self.left:
			value = []
			for r in self.right.glob(str(getattr(l, left._name))):
				if udp(l, r):
					value.append(r)
			setattr(l, self.alias, value)
			yield l

	def udf(self, udf):
		"""
User-Defined Functions provide for the most flexible join operations available in Underverse.

.. seealso::

	Look at the class description for usage

		"""
		for l in self.left:
			value = udf(l, self.right)
			setattr(l, self.alias, value)
			yield l

	def on(self, left, right, omit=False, default=[]):
		"""
The *ON* methodology mimics what is found in traditional RDBMS and SQL.

.. seealso::

	Look at the class description for usage

		"""
		groups = {}
		for r in self.right:
			key = str(getattr(r, right._name))
			if not key in groups:
				groups[key] = []
			groups[key].append(r)
		for l in self.left:
			key = str(getattr(l, left._name))
			if key in groups and not omit:
				setattr(l, self.alias, groups[key])
				yield l
			else:
				setattr(l, self.alias, default)
				yield l

if __name__ == '__main__':
	from underverse import *
	# import Model as F

	uv = Underverse()
	verse = uv.test

	emails = ['gmail.com', 'hotmail.com', 'yahoo.com', 'gmail.com', 'gmail.com']
	a = range(5)
	b = range(5)

	verse.from_array(zip(a, b, emails), names=['x', 'y', 'email'])

	for doc in verse:
		print doc

	# print Model.__dict__

	# print Model.__getattr__('name')

	# print Model('name') < 5
	# print Document.age < 5, Document.age <= 5
	# print Document.age < 5
	# print Document.age <= 5
	# print Document.age > 5
	# print Document.age >= 5
	# print Document.age == 5
	# print Document.age != 5
	# print Document.name.len(5)
	# print Document.age.btw(20, 50)
	# print Document.age

	# print Document.name.type(float)
	# print Document.name.in_(['Max'])
	# print Document.name.nin(['Max'])
	# print Document.name.match('Max')
	# print Document.name.search('ax')
	# print Document.name.nmatch('Max')
	# print Document.name.nsearch('Max')

	def email_search(value, domain=None):
		if domain in value:
			return True
		else:
			return False

	# print Document.email.udp(email_search, domain='gmail')

	print
	for d in verse.find(Document.email.udp(email_search, domain='gmail')):
		print d

	print
	# for d in verse.find_one(Document.email.udp(email_search, domain='gmail')):
	# 	print d

	print 
	for d in verse.find(Document.email.search('gmail')):
		print d

	def sq_filter(array, goal=2):
		subset = []
		for doc in array:
			if doc.x ** doc.y > goal:
				subset.append(doc)
		return subset

	for d in verse.find(Document.udf(sq_filter, 4)):
		print d

	# for d in verse.find_new(Model.email.udp(email_search, domain='gmail')):
		# print d
