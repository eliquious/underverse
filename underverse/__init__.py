"""
Underverse is named after the destination and afterlife of the Necromonger's from the
Chronicles of Riddick. This module is so named because the author likes the movie.

The Underverse is an Object Document Mapper, or ODM. An ODM performs roughly the same
functions for unstructured data as an ORM (Object Relational Mapper) does for relational
data minus the 'relationships'...

In Python, SQLAlchemy is the ORM of choice. The Underverse follows the same
design principles seen in SQLAlchemy. So if you've used it before, this module
should feel right at home.

.. note::

  Underverse is now **beta** due to the arrival of v0.3. However, there may still be uncovered bugs or unexpected errors waiting to be found. I'm continually working to increase the test coverage and to update the docs.

  If bugs are found, please join the `Google Group <http://groups.google.com/group/python-underverse>`_ and post the issue. Suggestions are also welcome. The project will be on github soon.


.. warning::

  Underverse underwent a tremendous maturation process between versions 0.2.3 and 0.3 and again between 0.3.6 and 0.4.0. Therefore, if you downloaded an earlier version, it would greatly benefit you to upgrade to the latest version.

  If you are using any ``mapreduce`` functionality, version 0.4.0 will likely fix any issues you have found.


What is Underverse?
===================

At it's core, the Underverse is a JSON-based document store built on top of SQlite. JSON was chosen over pickle because of it's size, simplicity, readability and speed. These features, combined with SQlite, make for a simple and elegant solution. Everything in the Underverse is written to use Python's generators to provide for a streamlined process for data access and manipulation. The module provides ways to quickly load, update, group, map and filter data.

To avoid some mis-conceptions, here's what it is and what it's not:

**Underverse is**:

  * Fast, easy, simple and clean
  * A zero-configuration solution
  * Extremely light-weight
  * Specialized

    * Think of a Knife, not a Low Orbit Ion Cannon
    * Agile, not encumbered by over-bearing features

  * Lean, very lean
  * **A wicked-sick, data-analysis module**
  * **NOT** a do-everything module
  * **NOT** into *keeping up with the Joneses*
  * **NOT** a NoSQL client-server solution

    * if you want one, you'd have to wrap it yourself (or use one that already works and save yourself a headache)

  * **NOT** a full NoSQL solution (at least not in the traditional sense)

    * it has no indexes
    * it has no built-in replication
    * it has no sharding-capability

  * **NOT** a solution for anyone with high-availability needs (again, not a client-server solution)

    * However, if an on-disk SQlite database is good enough for you, awesome.


The Underverse was designed to be **an analysis module**. It was engineered to be a defense for low-strafing questions on Friday @ 445 PM. It was built to answer questions *FAST*. With this in mind and by using NoSQL / Post-modern document storage principles, the Underverse can evolve over time allowing for increasingly deeper questions to be asked of the data it houses.

With this in mind, Underverse has now reached a stable enough development status that it could be used as the backend for websites and other data storage platforms.

.. ::

  **Author's Advice**:

    You'll do yourself a favor by remembering what this module was designed for. Every weapon has a specific purpose, use them for what they were built for in order to construct your own arsenal for dispatching problems and questions from customers.

  Do that and your boss won't know whether to hug you or kiss you. Hopefully neither if your boss is like most. That's also assuming you are not his (or her - for the PC crowd) secretary... Heck, you might even land a raise or two or three.

*"You keep what you kill."* - the late Lord Marshal

.. ::

  Questions are your prey.

Introduction
============

These next few sections outline possible points of familiarity for those who have worked with relational databases or other NoSQL data stores. I've also included a section comparing Underverse and SQL-Alchemy. As I've said before, if you've used SQL-Alchemy before then you should see some similarities.

Organization / Lingo
--------------------

Here are the relationships between the objects found in Underverse and a traditional RDBMS, as well as their counter-parts using NoSQL-speek:

.. raw:: html

  <br \>
  <div style="width:90%" id="main">
    <table class="ctable" id="if_class_comparison">
        <thead>
          <tr>
              <td>RDBMS</td>
              <td>NoSQL-speek</td>
              <td>Underverse</td>
          </tr>
        </thead>
      <tbody>
          <tr>
            <td>Database</td>
            <td>Database</td>
            <td>Underverse</td>
          </tr>
          <tr>
            <td>Table</td>
            <td>Collection</td>
            <td>Verse</td>
          </tr>
          <tr>
            <td>ResultSet</td>
            <td>-</td>
            <td>SubVerse</td>
          </tr>
          <tr>
            <td>ResultRow</td>
            <td>Document</td>
            <td>NecRow</td>
          </tr>
      </tbody>
    </table>
  </div>
  <br \>

SQL-Alchemy Comparison
----------------------

Underverse's query syntax is similar to SQLAlchemy's. However, Underverse removes some of the complexity of getting started.

But before I go any further, understand that Underverse is NOT meant to battle SQL-Alchemy in hand-to-hand combat.
There are many, many things that SQL-Alchemy does that are top-notch in the ORM world, most of which will never be covered in Underverse.
I have really enjoyed using SQL-Alchemy in the past. However, some, perhaps even most, projects can get away with using a much
lighter-weight approach to managing data. Underverse is meant to fill that niche.

Here's a comparison for those of you who are familiar to SQL-Alchemy:

**SQLAlchemy**::

  from sqlalchemy import create_engine, Column, Integer, String
  from sqlalchemy.ext.declarative import declarative_base
  from sqlalchemy.orm import scoped_session, sessionmaker

  Base = declarative_base()

  class User(Base):
      __tablename__ = 'users'

      id = Column(Integer, primary_key=True)
      name = Column(String)
      fullname = Column(String)
      password = Column(String)

      def __init__(self, name, fullname, password):
          self.name = name
          self.fullname = fullname
          self.password = password

      def __repr__(self):
         return "<User('%s','%s', '%s')>" % (self.name, self.fullname, self.password)

  session = scoped_session(sessionmaker(bind=create_engine('sqlite:///:memory:', echo=False)))

  ed_user = User('ed', 'Ed Jones', '3d5_p455w0r6')
  session.add(ed_user)

Let's take a look at everything that's happening. Before you can do anything you
have to define a data model. In the example case, the ``User`` class is defined
with first name, full name and password fields.

Next, a session is created to operate on. Then a ``User`` object is instantiated
and finally added to the session.

**Underverse**::

  from underverse import Underverse

  uv = Underverse()
  users = uv.users

  ed_user = { 'name': 'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'}
  users.add(ed_user)

Ok, at first glance you can see a large difference in the lines of code required.
Let's talk about what Underverse is doing.

When ``uv = Underverse()`` is called, a connection to an in-memory SQLite database
is made behind the scenes. Then a table is created by using
``users = uv.user``. No class structure or data model is needed to insert data.
Then to insert data, you simply call ``users.add(ed_user)``.

However, with the advent of v0.3, class objects are officially supported. Therefore, you can also do this.

.. code-block:: python

  uv = Underverse()
  test = uv.test

  class User(object):
    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password

  ed_user = User('ed', 'Ed Jones', '3d5_p455w0r6')
  test.add(ed_user)

Storing class instances are as painless as can be. **There is NO need for a base class** (unless you have need of one, of course).

.. note::

  Yes, I understand that SQL-Alchemy is an ORM and Underverse is not. The above was simply
  to show the differences of loading data.

  Please, don't send me hate mail.

Querying Preview
----------------

Using the same SQL-Alchemy code above, here's how querying is handled in both frameworks. Again, this is NOT a replacement for SQL-Alchemy, but rather a lighter-weight approach data storage and retrieval.

**SQL-Alchemy**::

  session.query(User).filter(User.fullname=='Ed Jones')

This statement finds all rows in the ``users`` table where ``fullname == 'Ed Jones'``.

Similarly **Underverse** does this::

  from underverse.model import Document
  users.find(Document.name == 'ed')

When using Underverse, the ``Document`` class is the main query object. It handles all document queries for every document collection. There's no need to create a class for every table or collection.

Additional Key-Value Functionality
----------------------------------

Underverse now has basic key-value storage methods. This can be used for many things, however, perhaps storing script options might be useful along with using the module for it's document storage capabilities as well.

Here's a preview of the key-value functionality. The ``KeyValue`` class will have a more detailed description of the existing capability.

.. code-block:: python

  uv = Underverse()
  options = uv.options

  # Add key by using the ``put`` method
  options.put('key', 'value')

  # Get the associated value
  print options.get('key')

  #This raises a KeyError.
  print options.get('key2')

MapReduce Functionality
-----------------------

.. note::

  **v0.4.0 is the version of choice for MapReduce operations.** This version contained several bug fixes and added capability for MapReduce.

Underverse has a powerful grouping capability built-in, however, if you have need of more specific control over what is grouped or aggregated, then MapReduce is for you. 

This module has 3 main functions for MapReduce:

* `map <http://packages.python.org/underverse/#underverse.Verse.map>`_: aggregates documents based on a function
* `reduce <http://packages.python.org/underverse/#underverse.Verse.reduce>`_: calls a reduce function for each group returned from a ``map``
* `simple_reduce <http://packages.python.org/underverse/#underverse.Verse.simple_reduce>`_: simply calls a function to operate on the entire dataset

For convenience, Underverse also has a `mapreduce <http://packages.python.org/underverse/#underverse.Verse.mapreduce>`_ function. This chains the ``map`` and ``reduce`` functions together for ease of use.

.. seealso::

  Click on the links above to see examples.

Module Documentation
====================

"""
import sqlite3, uuid, time, jsonpickle
from underverse.predicates import Predicate as P#, Sorter
from underverse.model import *
from underverse.ordereddict import OrderedDict
from underverse.handlers import *

__version__ = '0.4.1'
__author__ = 'Max Franks'


try:
  import numpy as np
  HAS_NUMPY = True
except ImportError:
  HAS_NUMPY = False


__all__ = ['NecRow', 'Underverse', 'Verse', 'SubVerse', 'and_', 'or_', 'adapter', 'converter']

## TODO
#
# add column mapping capability using a list of predicates,
#   if a predicate is false, it goes to the next one until there are no more. The default column mapping is Column1 - ColumnN
#   the predicates evaluate each line and return a list of strings if it meets certain criterion, otherwise it returns false
#
#+ add connection option for non-memory sqlite files
#+ add groupby functionality
#- add verse slicing capability using sqlite rowid or stream
#+ add a purge all function
# add logging capability
#   no glob results, # of
#    found #
#+ add update all functionality, like add_all
#+ add where select functionality
#+ and/or functionality?
#+ add map functionality
#+ add reduce functionality
#+ merge add / add_all functions

#+ implement paging

#- subclass stream?
#-   no, overload operators
#
#bulk insert: verse << list of rows
#insert: verse < row
#update: row < dict, row + dict
#remove: verse - row,
#purge: verse >> NULL (new class)

#+add sqlalchemy-ish functionality
#+   operators on column names
#+   Predicates with overloaded operators? No
#
#+ Document class with operator overloading which returns a predicate for filtering
#   Much more readable
#
#   Document.name == 'Max'
#   ('name', ['eq', 'Max'])
#

# add to numpy functionality using np.fromiter(data, dtype, count=len(data))
#+ add functionality to add/update an entire column
#+  def verse.add_column(data, name, commit=True):
#   if len(data) != len(self.data):
#      raise Exception, "column data must be the same length as existing data"
#
#+  def verse.add_columns(data, names, commit=True):
#   if len(data) != len(self.data):
#      raise Exception, "column data must be the same length as existing data"
#
#-base class

#Performance
#??? both the unique and mapper verse functions have callback capability
#limskip is fast, slicing is faster after data filter has been persisted

#+possibility of removing stream from dependencies
#needed? use versioning to persist changes to data
#needed?   add a purge (clean) functionality to remove older versions

#Data Validation
#Sub-class unittest
#RuleSets and Rules
#RuleSet - parent class
# - has rules
# - rules can be applied to datasets to filter / flag conditional criterion
# - data attributes are used to store rule results
# - - rules -> RuleSet -> Rule
#Rule decorator

# Operand Enumeration
and_ = ' and '
or_ = ' or '

class NecRow(dict):
  """

Each document is based on a Python dict object. When interacting with SQlite, this class is the storage object. Python classes can be persisted using Underverse as long as they have a *__dict__* function. If *object* is the super class you should be fine.

When documents are persisted, they are given a *UUID* as well as *created_at* and *updated_at* properties. This is why the data persisted is slightly larger than the raw data.

The persisted object can both get and set variables easily using the *dot* syntax.


.. code-block:: python

  #connect to existing document store
  uv = Underverse("existing.db")

  #each Verse (or SQlite table) has iterator functionality
  for person in uv.people:

    #Get first name and last name
    first = person.first_name
    last = person.last_name

    #set full name
    person.full_name = "%s %s" % (first, last)

  """
  def __init__(self, *args, **kwargs):
    super(NecRow, self).__init__(*args, **kwargs) #*args, **kwargs
    # self.update(*args, **kwargs) # use the free update to set keys

    if not "uuid" in self:
      self["uuid"] = str(uuid.uuid4())
    if not "created_at" in self:
      self["created_at"] = time.time()
    if not "updated_at" in self:
      self["updated_at"] = self["created_at"]

  def __getattr__(self, attr):
    def nested(_attr):
        attrs = _attr.split('.')
        value = self.__getattr__(attrs[0])
        if value is None:
          raise AttributeError, "Attribute not found: '%s'" % str(attrs[0])
        else:
          attrs.pop(0)
          for a in attrs:
            try:
              value = getattr(value, a)
            except Exception, e:
              raise AttributeError, "Attribute not found: '%s'" % str(attr)
              # raise KeyError, "Document does not have a key named '%s' " % (key)
          return value
    try:
      if attr in self:
        return self[attr]
      elif '__data__' in self:
        if type(self['__data__']) == dict and attr in self['__data__']:
          return self['__data__'][attr]
        elif hasattr(self['__data__'], attr):
          return getattr(self['__data__'], attr)
        elif '.' in attr:
          return nested(attr)
      elif 'value' in self:
        if hasattr(self['value'], attr):
          return getattr(self['value'], attr)
        elif type(self['value']) == dict and attr in self['value']:
          return self['value'][attr]
        elif '.' in attr:
          return nested(attr)
        return self['value']
      elif '.' in attr:
        return nested(attr)
    except:
      raise #AttributeError, "Attribute not found: '%s'" % str(attr)

  def __setattr__(self, attr, value):
    self[attr] = value

  def __dict__(self):
    return self

class KeyValue(NecRow):
  """
This class adds support for storing key-value documents.

The key-value storage is built on top of the current document
infrastructure. Each document has a *UUID*, a *created_at* and
*updated_at* attributes.

Key-Value documents override the UUID variable with a custom
user-defined value. Where as the 'value' portion is stored
like all other documents.

Adding and getting keys and there values is simple:

.. code-block:: python

  uv = Underverse()
  options = uv.options

  # Add key by using the ``put`` method
  options.put('key', 'value')

  # Get the associated value
  print options.get('key')

  #This raises a KeyError.
  print options.get('key2')

Here's another way to do it, but it's not the easiest. The first code example makes it easier. The following code gives a more verbose explanation.

.. code-block:: python

  from underverse import Underverse, KeyValue

  uv = Underverse()
  options = uv.options

  n = KeyValue('key', 5)
  options.add(n)

  # Key-Values can also be used as other documents as well...
  for o in options:
    print o
    o.value2 = o.value * 2
    options.update(o)

  # Basically, the following line is doing the same thing as a KeyValue instance
  # n = NecRow({'uuid':'key', 'value':5})

  """
  def __init__(self, key, value):
    super(KeyValue, self).__init__()
    self.uuid = key
    self.value = value

def adapter(data):
  return jsonpickle.encode(data)

def converter(s):
  return NecRow(**jsonpickle.decode(s))

# Register the adapter/converter
sqlite3.register_adapter(NecRow, adapter)
sqlite3.register_converter("necro", converter)

class SubVerse(object):
  """

If you are familiar to database programming, you can think of SubVerse
as something similar to a ResultSet object. Anytime a query is made
of a Verse (or collection) object, this is what is returned. Therefore,
this object contains most of the logic in terms of filtering, grouping and mapping data.

However, you can also use it without having to load data into an
Underverse instance. It will also operate on a list of dicts out of the box.

.. code-block:: python

  >>> dicts = [{'name':'ed', 'fullname':'Ed Jones', 'password':'3d5_p455w0r6'},
  ...     {'name':'wendy', 'fullname':'Wendy Williams', 'password':'foobar'},
  ...     {'name':'mary', 'fullname':'Mary Contrary', 'password':'xxg527'},
  ...     {'name':'fred', 'fullname':'Fred Flinstone', 'password':'blah'}]

  >>> for user in SubVerse(dicts).find(Document.name == 'ed'):
  ... print user
  {'fullname': 'Ed Jones', 'password': '3d5_p455w0r6', 'name': 'ed'}

Notice that the found document doesn't have a *UUID*, *created_at* or *updated_at* keys. This is because it was never loaded into SQlite.

  """
  def __init__(self, division):
    super(SubVerse, self).__init__()
    self.division = division

  def find(self, *filters):
    """
Filters a list of NecRows based on input conditions searches verse for true predicates

.. seealso::

  You can look at the find function for *Verse* to get an example on how to query.

    """

    if len(filters) == 0:
      return self

    def filter_(array, attr, predicate):
      true = []
      for row in array:
        if attr in row or hasattr(row, attr):
          if predicate(row):
            true.append(row)
      return true

    result = iter(self)
    for _filter in filters:
      if hasattr(_filter, '__predicate__'):
        result = filter_(result, _filter._name, _filter._predicate)
      elif callable(_filter):
        result = _filter(result)
      else:
        raise TypeError, "Filter given isn't recognized"

    return SubVerse([r for r in result])

  def find_one(self, *filters):
    """
    Executes the same way as the ``find`` function, but only returns the first document.

    Returns None if no document is found matching conditions

    .. seealso::

      Look at the *find_one* function for Verse to see an example.

    """
    fs = list(filters)
    fs.append(Document.limit(1))
    one = self.find(*fs)
    if len(one) == 1:
      return one[0]
    return None

  def paginate(self, count):
    """
    Pages the collection.

    If a collection has 5000 documents, calling ``verse.paginate(500)`` will return 10 pages of 500 documents each.

    .. seealso::

      Look at the *paginate* function for Verse to see an example.
    """
    page = []
    for doc in self:

      if len(page) % count == 0 and len(page) > 0:
        yield page
        page = []
      page.append(doc)
    if len(page) > 0:
      yield page

  def __getattr__(self, attr):
    for a in self.division:
      if attr in a or hasattr(a, attr):
        yield getattr(a, attr)

  def purify(self, *cols):
    """
Converts the given attributes to a NumPy recarray. This can provide a speed boost for data filtering, grouping and manipulation.

.. seealso::

  Please look at the Verse 'purify' documentation on how to use this functionality.

    """
    if HAS_NUMPY:
      return QuasiDead.from_dicts(self.division, *cols)
    else:
      raise Exception, "NumPy must be installed to use this function."

  def __len__(self):
    return len(self.division)

  def __iter__(self):
    return iter(self.division)

  def __getitem__(self, _slice):
    return self.division[_slice]

  @staticmethod
  def __expandmap__(mapped_results, wrap=True):
    '''
    This class expands a dictionary, where the keys are iterables,
      into a list with the mapped values as the last index.
    '''
    expanded_results = []
    for k, v in mapped_results.items():
      tmp = []
      if hasattr(k, '__iter__') and type(k) != str:
        tmp.extend(k)
      elif k is None:
        tmp.append(k)
      else:
        if len(k) > 0 and type(k) != str and type(k) != unicode:
          for l in k: tmp.append(l)
        else:
          tmp.append(k)
      if wrap:
        tmp.append(SubVerse(v))
      else:
        tmp.append(v)
      expanded_results.append(tmp)
    return expanded_results

  def groupby(self, *attrs):
    """
Grouping data can be extremely powerful in data analysis. Therefore, data grouping and aggregation
in Underverse does not hold back nor does it disappoint.

.. seealso::

  Please look at the *groupby* functionality for a Verse for a more comprehensive usage explanation.

    """
    groups = []
    for attr in attrs:
      if not attr in groups:
        if type(attr) == Document:
          groups.append(attr._name)
        elif type(attr) == str:
          groups.append(attr)
        else:
          raise TypeError, "Group by arguments must be either str or Document types"
    data = {}
    for a in self:
      key = []
      for attr in groups:
        if attr in a:
          key.append(a[attr])
        elif hasattr(a, attr):
          key.append(getattr(a, attr))
      if not tuple(key) in data:
        data[tuple(key)] = []
      data[tuple(key)].append(a)

    return SubVerse.__expandmap__(data)

  def unique(self, *attrs):
    """
    Finds all the unique values for one or more columns

    .. seealso::

      Please look at the *unique* functionality for a Verse for a more comprehensive usage explanation.
    """
    names = {}

    if len(attrs) == 0:
      raise TypeError, "Unique function requires data attributes to group on"
    elif len(attrs) == 1 and callable(attrs[0]):
      for k in attrs[0](self):
        if not tuple(k) in names:
          names[tuple(k)] = 0
          yield k
    elif len(attrs) == 1:
      attr = attrs[0]
      for a in self:
        if attr in a and not a[attr] in names:
          names[a[attr]] = 0
          yield a[attr]
        elif hasattr(a, attr):
            val = getattr(a, attr)
            if not val in names:
                names[val] = 0
                yield val
    else:
      def func(array, attrs):
        for a in array:
          ks = []
          for attr in attrs:
            if type(attr) is str:
              ks.append(a[attr])
            if type(attr) is Document:
              ks.append(a[attr._name])
          yield tuple(ks)

      for k in func(self, attrs):
        if not tuple(k,) in names:
          names[tuple(k,)] = None
          yield k

  def map(self, function, wrap=True, expand=True):
    """
    Calls a map function on the subverse.

    Mapper functions must or yield a two-value tuple.
    This tuple represents a key-value pair. The key will be used to
    aggregate similar data. The value can be anything, but remember
    you will have a list of these values for every key.

    .. seealso::

      Please look at the *map* functionality for a Verse for a more comprehensive usage explanation.

    .. note::

      Bug fixes in v0.4.0
    """
    names = OrderedDict()

    for k, v in function(self):
      if not k in names:
        names[k] = [v]
      else:
        names[k].append(v)
    if expand:
      return SubVerse.__expandmap__(names, wrap)
    else:
      return names.items()
      # for k, v in names.items():
      #   if wrap:
      #     yield k, SubVerse(v)
      #   else:
      #     yield k, v

  def simple_reduce(self, reducer):
    """
This fuction simply returns the result of passing the entire dataset to the *reducing* function.

.. code-block:: python

  col = uv.collection
  count = col.simple_reduce(len)

Python's ``len`` function is passed the entire dataset and returns the count. Reducers
are menat to return a single result for the entire dataset. However, no one enforces that
rule. You could return a list object and no one would care.

.. note::

  Bug fixes in v0.4.0
    """
    return reducer(self)

  def reduce(self, mapper_results, reducer, expand=True, sort=True):
    '''
This function 'reduces' the data returned from each map group.
Reducers are meant to return a single value per group. However, due to python's
typing you can return a list, dictionary or tuple because they are objects themselves.

.. seealso::

  Look at the documentation for ``Verse.reduce`` for a longer explaination.

.. note::

  Bug fixes in v0.4.0

    '''
    d = OrderedDict()
    if reducer is not None:
      for key, group in mapper_results:
        d[key] = reducer(group)
    else:
      raise TypeError, "'reduce' argument cannot be 'None'. It must be callable."

    if type(sort) is bool:
      if sort == True:
        d = OrderedDict(sorted(d.items()))
    elif type(sort) is str and len(sort) > 0:
      if sort.strip().lower() == 'map':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[0]))
      elif sort.strip().lower() == 'reduce':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[1]))
      elif sort.strip().lower() == '-map':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[0], reverse=True))
      elif sort.strip().lower() == '-reduce':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[1], reverse=True))
      else:
        raise ValueError, "The 'sort' argument can either be a boolean value or a string \n containing 'map' or 'reduce'. \n'"+str(type(sort))+"' arguments are not accepted."

    if not expand:
      return d.items()
    else:
      return SubVerse.__expandmap__(d, wrap = False)

  def mapreduce(self, mapper, reducer, expand=True, sort=True):
    '''
This function calls the map and reduce functions and returns
the results as a dictionary.

However, if the **expand** option is ``True``, the results will be
expanded into a list. This means that each row in the results
will be a tuple, with the last value being the mapped data
(returned as the value from the mapper function).

.. seealso::

  Look at the documentation for ``Verse.mapreduce`` for a longer explaination.

.. note::

  Bug fixes in v0.4.0
    '''
    return self.reduce(self.map(mapper, expand=False), reducer, expand, sort)

  def orderby(self, *attrs):
    """
Orders a SubVerse by the attributes given
    """
    groups = []
    for attr in attrs:
      if not attr in groups:
        if type(attr) == Document:
          groups.append(attr._name)
        elif type(attr) == str:
          groups.append(attr)
        else:
          raise TypeError, "Order by arguments must be either str or Document types"
    if len(groups) == 0:
      return sorted(self)
    return P.orderby(*groups)(self)
    # return sorted(self, key=lambda x: tuple([getattr(x, arg) for arg in groups]))

  def limit(self, count):
    """
    Returns a user-defined limited number of documents.
    """
    return self.find(Document.limit(count))

  def skip(self, count):
    """
    Returns all documents after the user-defined number have been skipped.
    """
    return self.find(Document.skip(count))

  def join(self, right, alias):
    """
This performs join operations on the current SubVerse.
    """
    return Join(self.division, right, alias)

class Verse(object):
  """
A Verse is a class which represents a collection of similar data.
If you're familiar to traditional relational databases, it's like a table.
It can also be described as a document collection. In the background, it's
actually a SQlite table. This class allows for standard CRUD
(create, read, update and delete) operations.

.. code-block:: python

  uv = Underverse()

  #there are two ways to create a new document collection
  members = uv.members

  #or...
  accounts = uv.new("accounts")

The code above creates a new collection if it doen't currently exist
or connects to an existing collection.

  """
  def __init__(self, connection, name):
    super(Verse, self).__init__()
    self._connection = connection
    self._name = name
    self._len = -1
    self._dirty = True

    # self.connection.create_function("has", 2, has)
    # self.connection.create_function("get", 2, get)
    # self.connection.create_function("eq", 3, eq)
    self._cursor = self._connection.cursor()
    self._cursor.execute("create table if not exists %s (uuid unique, data necro);" % name)

  def add_column(self, array, name, commit=True):
    """
Adds an array with column names to the dataset.

.. note::

  The given array must be the same size as the entire
  collection. If this doesn't fit with what you are
  trying to do, just use the included update functionality.

    """
    if not hasattr(array, '__iter__'):
      raise TypeError, "Array must be iterable. The new column data is the 1st argument"
    if  len(array) != len(self):
      raise ValueError, "Input column must be the same size as the existing data. array: %s, existing: %s" % (len(array), len(self))
    if not type(name) is str:
      raise TypeError, "Column name must be a string"

    count = 0
    updates = []
    for a in self:
      a[name] = array[count]
      count += 1
      updates.append(a)

      if commit and count % 2500 == 0:
        self.update(updates)
        updates = []

    if commit:
      self.update(updates)

    return SubVerse(self)

  def from_array(self, array, names, commit=True):
    """

Adds an array with column names to the dataset.

.. code-block:: python

  uv = Underverse()
  table = uv.collection

  array = [[1,2,3],[4,5,6],[7,8,9]]

  table.from_array(array, names=['x', 'y', 'z'])

.. todo::

  Add predicate naming functionality. Every row will be passed
  through each conditional function and return column names
  if it's criterion is met. If no predicate matches the data,
  either the row is skipped or is given default column names
  (col1 - colN).

    """
    if not hasattr(array, '__iter__'):
      raise TypeError, "Array must be iterable"
    if len(names) != len(array[0]):
      raise ValueError, "Names and array arguments must have the same number of columns"

    data = []
    for a in array:
      row = {}
      for i, j in enumerate(a):
        row[names[i]] = j
      data.append(row)

    if commit:
      self.add(data)
    return SubVerse(data)

  def add(self, necro):
    """
Adds a NecRow or a list of NecRows to the database.

.. code-block:: python

  uv = Underverse()
  table = uv.helion

  #you can either add one row at a time
  table.add({'a':1, 'b': 2})

  # or do bulk inserts
  array = [
    {'a':1,'b':2,'c':3},
    {'a':4,'b':5,'c':6},
    {'a':7,'b':8,'c':9}]

  table.add(array)

.. admonition:: Performance Hint
  :class: perf


  Bulk inserts are noticibly faster

    """
    self._dirty = True

    try:
      if hasattr(necro, '__iter__') and type(necro) != NecRow and type(necro) != dict and not isinstance(necro, NecRow):
        def generator(new_converts):
          for n in new_converts:
            # print type(n)
            if type(n) is not NecRow:
              # print n
              if not hasattr(n, '__dict__') and not type(n) is dict:
                necro = NecRow({'__data__':n})
              elif type(n) is dict:
                necro = NecRow(n)
              elif hasattr(n, '__dict__') or isinstance(n, NecRow):
                  necro = NecRow()
                  necro.update({'__data__':n})
              else:
                necro = NecRow(dict(n))
              yield (str(necro.uuid), adapter(necro),)
            else:
              yield (str(n.uuid), adapter(n),)

        self._cursor.executemany("insert into %s (uuid, data) values (?, ?)" % self._name, generator(necro))

      elif type(necro) is NecRow or isinstance(necro, NecRow):
        self._cursor.execute("insert into %s (uuid, data) values (?, ?)" % self._name, (necro.uuid, adapter(necro)))
      elif type(necro) is dict:
        necro = NecRow(dict(necro))
        self._cursor.execute("insert into %s (uuid, data) values (?, ?)" % self._name, (necro.uuid, adapter(necro)))
      elif hasattr(necro, '__dict__'):
        necro = NecRow(necro.__dict__)
        self._cursor.execute("insert into %s (uuid, data) values (?, ?)" % self._name, (necro.uuid, adapter(necro)))
      else:
        raise ValueError, "Document could not be loaded. Please look at documentation for examples of loading data."
    except:
      raise
      # raise TypeError, "Add argument must be of type NecRow or a list of NecRows"

    self._connection.commit()
    return self._cursor.execute("select changes();").fetchone()[0]

  def all(self):
    """
Returns a SubVerse object containing all objects in the verse / collection

    """
    return SubVerse(self)

  def __getattr__(self, attr):
    """

You can also get a list for a particular data attribute for the entire collection.

.. code-block:: python

  uv = Underverse("existing.db")

  #connect to the table
  members = uv.members

  for last_name in members.last_name:
    print last_name

The code above will loop over the entire collection of members and
print the 'last_name' variable for each document. If a document does
not have the 'last_name' attribute it is currently skipped.

.. todo::

  Future versions may include a default value for documents not
  containing a given attribute.

    """
    return getattr(self.all(), attr)

  def __iter__(self):
    for n in self._cursor.execute('select data as "data [necro]" from %s' % self._name):
      yield n[0]

  def groupby(self, *attrs):
    """
The grouping of similar data is essential to most
data analysis operations. This function is similar in
nature to the *map* function with perhaps more readable syntax.

The *groupby* function doesn't have as much power as the *map* function,
however, this function aggregates data based on one or more attributes.

The map capability allows for more freedom with the possibility of
calculated key-value pairs.

.. code-block:: python

  uv = Underverse()

  members = uv.members

  #insert data
  #...
  members.add(list_of_ppl)

  for state, inhabitants in members.groupby('state'):
    print "State: %s" % state
    print " - Population: %s" % len(inhabitants)

The code above will print all the states that have any
members as it's citizens. The 'inhabitants' variable is a
SubVerse instance containing all the citizens for the given
state.

You can group by more than one column as well, such as ``members.groupy('state', 'county')``.

    """
    return self.all().groupby(*attrs)
    # return SubVerse(iter(self)).groupby(*attrs)

  def orderby(self, *attrs):
    """
Orders the collection by one or more columns

.. code-block:: python

  uv.docs.orderby('name', '-age')

The ``orderby`` functionality now has *ASC* and *DESC*
capability. Descending order is achieved by pre-pending
a '**-**' (negative sign or hyphen) to the column name.

However, you can also do this:

.. code-block:: python

  uv.docs.find(D.orderby('name', '-age'))

    """
    return P.orderby(*attrs)(self)
    # return P.orderby(*attrs)(SubVerse(self))
    # return SubVerse(self).orderby(*attrs)

  def unique(self, *attrs):
    """
Finds all the unique values for one or more columns

.. code-block:: python

  uv.verse.unique('name', 'age')

    """
    return self.all().unique(*attrs)
    # return SubVerse(iter(self)).unique(*attrs)

  def __len__(self):
    """

Returns the number of documents for the entire collection

    """
    if self._dirty or self._len == -1:
      self._dirty = False
      self._len = self._cursor.execute('select count(*) from %s' % self._name).fetchone()[0]
    return self._len

  def find(self, *filters):
    """
Searches verse for documents that meet all the predicates given.
A SubVerse instance is returned containing all the found documents.
This is how querying is handled in the Underverse.

**Usage**::

  #create in-memory database
  uv = Underverse()

  # this loads a previous dump of an Underverse instance (`uv.dump("backup.sql")`)
  uv.load("backup.sql")

  #Document instances are the 'query' objects in the Underverse.
  #Unlike SQLAlchemy, each table doesn't need to have a model.
  #The Underverse uses one model object for all tables...
  #The 'country_code' is the attribute (or 'column' in traditional RDBMS) you are searching for.
  #In this case, the coder is searching for all members in the US.
  for person in uv.members.find(Document.country_code == "US"):
    print person

    #calculate stats
    #...

  #closes the connection
  uv.close()


Look at the `Document` documentation for all supported query operators.

    """
    return Verse(self._connection, self._name).all().find(*filters)

  def find_one(self, *filters):
    """

Searches verse for the first document that is true for all predicates given

    """
    return self.all().find_one(*filters)

  def glob(self, globs, case_sensitive=True, operand=and_):
    """
Searches verse using SQlite GLOB statements. This is used to limit the number of documents being queried for.

By importing ``and_`` or ``or_`` from ``underverse``, you can perform more specific glob operations.

**Example usage:**

For example, say you have an unstructured CSV (comma separated values) file with
network traffic. The file has several different types of logging messages with
both received and transmitted messages. You need to search for a specific
logging message, but there's a lot of data and converting all the extra messages
would be pointless and time consuming. Here's how you would filter out the extra
messages and then group by IP address.

.. code-block:: python

  uv = Underverse()
  uv.load("logs.sql")
  logs = uv.logs

  #finds all WARNING messages that were received
  warnings_received = logs.glob('rxnet', 'WARNING')

  # groups the warnings by IP
  for ip, warnings in warnings_received.groupby("ip"):
    print '\\nIP Address: ' + str(ip)

    for warning in warnings:
      print ' - Description: ' + str(warning.desc)

.. admonition:: Performance Hint
  :class: perf


    Use this whenever you can to gain a slight performance boost. Because this
    function uses SQlite's GLOB functionality, the documents can be searched before
    they are converted to python dictionary.

    However, querying and globbing speed can be increased by intellegently ordering query parameters.
    In Underverse, if one condition fails, then the record is skipped without processing the other conditions.
    Therefore, use the more strict conditions first.

  .. code-block:: python

    # globbing - 2.297s
    warnings_received = logs.glob('WARNING', 'rxnet')

    # faster globbing - 1.141s
    warnings_received = logs.glob('rxnet', 'WARNING')

    # querying - 2.313s
    warnings_received = logs.find(D.Message_Label == "WARNING", D.rx_tx_net == 'rxnet')

    # faster querying - 2.218s
    warnings_received = logs.find(D.Message_Label == "WARNING", D.rx_tx_net == 'rxnet')

  The code above searched 50k+ network messages and the times shown are the min of 3 runs.

    """
    attrs = []

    def helper(attr):
      attr = attr.strip()
      if not attr.startswith("*"):
        attr = "*"+attr
      if not attr.endswith("*"):
        attr = attr+"*"
      if not case_sensitive:
        attr = attr.lower()
        if not attr in attrs:
          return " lower(data) glob '"+attr+"'"
      else:
        if not attr in attrs:
          return " data glob '"+attr+"'"

    if hasattr(globs, '__iter__'):
      for glob in globs:
        attrs.append(helper(glob))
    elif type(globs) is str:
        attrs.append(helper(globs))
    else:
      raise TypeError, "GLOBs must be strings or a list of strings"

    result = self._cursor.execute('select data as "data [necro]" from %s where%s' % (self._name, operand.join(attrs)))
    return SubVerse([r[0] for r in result])

  def update(self, necro):
    """
Updates a document or a list of documents.


**One possible usage..**

Say you have a website where members can comment on blog posts. You
want to run some stats on how many posts each user has commented on.
But when you were building your site you didn't think about that...

Let's say that you have already loaded your members into an Underverse
instance. Being the brilliant coder you are, you have also given each
member a list of their comments as well. You just want to add a comment
count. Here's one way to do it.

.. code-block:: python

  # connect to SQlite database
  uv = Underverse("web.db")

  #select members table
  members = uv.members

  #update every member in collection
  for member in members:

    #assuming each member has a list of comments under member.comments
    #the same syntax is used for adding a new attribute of updating an existing attribute
    member.comment_count = len(member.comments)

    #update document: collection.update(document)
    members.update(member)

Bulk operations are normally faster. This is how you could do the same thing using bulk updates.

.. code-block:: python

  uv = Underverse("web.db")
  members = uv.members

  #create a list of documents to be updated
  updated = []

  #update every member in the collection
  for member in members:

    #assuming each member has a list of comments under member.comments
    #the same syntax is used for adding a new attribute of updating an existing attribute
    member.comment_count = len(member.comments)

    # add member to the list to be updated
    updated.append(member)

    # incremental updates can be achieved like so
    # updating every 2500 documents
    if len(updated) % 2500 == 0:
      members.update(updated)

      #don't forget to clear the queue
      updated = []

  # commit any remaining documents in queue
  members.update(updated)

.. admonition:: Performance Hint
  :class: perf


    Bulk updates are faster because SQlite is handling more of
    the work. Any time you can limit function calls, do it.

    """
    self._dirty = True
    if hasattr(necro, '__iter__') and type(necro) != NecRow:

      def generator(necros):
        for n in necros:
          if type(n) is not NecRow:
            if not hasattr(n, '__dict__'):
              necro = NecRow({'data':n})
            else:
              necro = NecRow(n.__dict__)
            yield (adapter(necro), necro.uuid,)
          else:
            n.updated_at = time.time()
            yield (adapter(n), n.uuid,)
      self._cursor.executemany("update %s set data=? where uuid=?;" % self._name, (generator(necro)))
    elif type(necro) is NecRow:
      necro.updated_at = time.time()
      self._cursor.execute("update %s set data='%s' where uuid='%s';" % (self._name, adapter(necro), necro.uuid))
    else:
      raise TypeError, "Update argument must be of type NecRow or a list of NecRows"

    self._connection.commit()
    return self._cursor.execute("select changes();").fetchone()[0]

  def remove(self, necro):
    """
Removes a documents or a list of documents.

This function operates just like the 'add' and 'update'
operations. It accepts either a single document or a
list of documents.

.. todo::

  Add exceptions to function (ie, raise TypeError for unrecognized arguments).

    """
    self._dirty = True
    if type(necro) == str:
      self._cursor.execute("delete from %s where uuid = '%s';" % (self._name, necro))
    elif hasattr(necro, '__iter__') and type(necro) != NecRow:
      concat = []
      for n in necro:
        concat.append("'"+n.uuid+"'")
      self._cursor.execute("delete from %s where uuid in (%s);" % (self._name, ','.join(concat)))
    elif hasattr(necro, 'uuid'):
      self._cursor.execute("delete from %s where uuid = ?;" % (self._name), necro.uuid)
    self._connection.commit()
    return self._cursor.execute("select changes();").fetchone()[0]

  def purge(self, vacuum=False):
    """
Removes all data in collection.

If you are using an on-disk database, you will notice than after the data has been
purged, the size on disk is not smaller. This is due to SQLite's handling of the data
structures. The storage space is left to be used by future documents.

To remove the data from the disk permanently, use the ``vacuum`` option.

.. code-block:: python

  verse.purge(True)

.. note::

  *Vacuuming* the database is an expensive task. You might want to use it sparingly.

    """
    self._dirty = True
    self._cursor.execute("delete from %s;" % (self._name))
    self._connection.commit()
    if vacuum:
      self._cursor.execute("vacuum %s;" % (self._name))
    return self._cursor.execute("select changes();").fetchone()[0]

  def map(self, function):
    """
    Calls a map function on the entire collection.

    Mapper functions must or yield a two-value tuple.
    This tuple represents a key-value pair. The key will be used to
    aggregate similar data. The value can be anything, but remember
    you will have a list of these values for every key.

    .. code-block:: python

      #groups documents by name and age
      def name_mapper(array):

        # iterates through all the documents and yields name, age and the entire document
        # depending on what you are trying to accomplish, the document may not need to be returned
        # returning a '1' as the value instead of the document could be used with the 'sum' function to count as well
        for doc in array:
          yield (doc.name, doc.age), doc

      # iterates through the map results
      for name, age, ppl in uv.test.map(name_mapper):

        # ppl is a list of documents with the same name and age
        print name, age, len(ppl)

    .. note::

      Bug fixes in v0.4.0
    """
    return self.all().map(function)
    # for k, v in SubVerse(self.all()).map(function):
      # yield k, v

  def simple_reduce(self, function):
    """
    Calls a reduce function on the entire collection.

    .. note::

      Bug fixes in v0.4.0
    """
    return self.all().simple_reduce(function)
    # for k, v in SubVerse(self.all()).reduce(function):
      # yield k, v

  def reduce(self, mapper_results, reducer, expand=True, sort=False):
    '''
This function 'reduces' the data returned from each map group.
Reducers are meant to return a single value per group. However, due to python's
typing you can return a list, dictionary or tuple because they are objects themselves.

.. code-block: python

    test = uv.test

    def name_mapper(array):
      for doc in array:
        yield doc.name, 1

    results = test.map(name_mapper)
    red = test.reduce(results, sum, sort='-reduce')

    for name, count in red[:5]:
      print name, count

There are severl points to make about the code above. First, ``map`` can be stored 
for later, meaning multiple reducers can run on the same map results. Second, 
all ``reduce`` and``mapreduce`` functions have both ``expand`` and ``sort`` options.

The ``expand`` option *expands* the keys from a map operation into a tuple for easy 
access along with the data it grouped.

The ``sort`` option has five possible values:

 * **True**: uses the built-in Python  ``sorted`` function to sort the results
 * **map**: sorts the results on the mapped keys
 * **reduce**: sorts the results on the reduced values
 * **-map**: sorts the results on the mapped keys in reverse
 * **-reduce**: sorts the results on the reduced values in reverse

 The *descending* or *negative* options allow for operations such as *Top 5 most frequent ...*.

.. note::

  Bug fixes in v0.4.0
    '''
    d = OrderedDict()
    if reducer is not None:
      for key, group in mapper_results:
        d[key] = reducer(group)
    else:
      raise TypeError, "'reduce' argument cannot be 'None'. It must be callable."
    # return new

    if type(sort) is bool:
      if sort == True:
        d = OrderedDict(sorted(d.items()))
    elif type(sort) is str and len(sort) > 0:
      if sort.strip().lower() == 'map':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[0]))
      elif sort.strip().lower() == 'reduce':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[1]))
      elif sort.strip().lower() == '-map':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[0], reverse=True))
      elif sort.strip().lower() == '-reduce':
        d = OrderedDict(sorted(d.items(), key=lambda x: x[1], reverse=True))
      else:
        raise ValueError, "The 'sort' argument can either be a boolean value or a string \n containing 'map' or 'reduce'. \n'"+str(type(sort))+"' arguments are not accepted."

    if not expand:
      return d.items()
    else:
      return SubVerse.__expandmap__(d, wrap = False)

  def mapreduce(self, mapper, reducer, expand=True, sort=False):
    """
    Calls map and reduce functions on the entire collection.

    .. code-block:: python

      #groups documents by name and age
      def name_mapper(array):

        # iterates through all the documents and yields name, age and the entire document
        # depending on what you are trying to accomplish, the document may not need to be returned
        # returning a '1' as the value instead of the document could be used with the 'sum' function to count as well
        for doc in array:
          yield (doc.name, doc.age), doc

      # iterates through the mapreduce results
      for name, age, count in test.mapreduce(name_mapper, len):

        print name, age, count

    The code above aggregates the names and ages together, then calls ``len`` on the grouped data.

    The ``expand`` option *expands* the keys from a map operation into a tuple for easy 
    access along with the data it grouped.

    The ``sort`` option has five possible values:

     * **True**: uses the built-in Python  ``sorted`` function to sort the results
     * **map**: sorts the results on the mapped keys
     * **reduce**: sorts the results on the reduced values
     * **-map**: sorts the results on the mapped keys in reverse
     * **-reduce**: sorts the results on the reduced values in reverse

     The ``desc`` or *negative* options allow for operations such as *Top 5 most frequent ...*.

    """
    return self.all().mapreduce(mapper, reducer, expand=expand, sort=sort)
    # for k, v in SubVerse(self).mapreduce(mapper, reducer, expand, sort):
      # yield k, v

  def paginate(self, count):
    """
    Pages the collection.

    If a collection has 5000 documents, calling ``verse.paginate(500)`` will
    return 10 pages of 500 documents each.
    """
    return self.all().paginate(count)
    # for page in self.all().paginate(count):
      # yield page

  def purify(self, *cols):
    """
Converts the given attributes to a NumPy recarray. This can provide for an easy transition to NumPy.

.. warning::

  The 'purify' function requires that NumPy is installed.

.. note::

  **Design History**:

    Just as an FYI, before Underverse was started, I worked on another analysis module
    called bops. *bops* has similar functionality as Underverse, however, it is
    limited to rectangular datasets. Bops used NumPy as it's foundation and was blazing fast
    because of it. However, it was also a memory hog. Many of the design features seen in
    Underverse were originally 'fleshed out' in *bops*.

    I have rewritten the core of *bops* and added it as an extension to Underverse.
    It is called the ``QuasiDead``.

    This extension allows for a subset of a Verse to be shoved temporarily into NumPy to give the
    coder access to the added functionality NumPy provides. In order for this to work, the chosen
    attributes are sliced from the document collection and molded into a structured data set
    which NumPy can handle.

    Each ``QuasiDead`` instance can also be grouped, ordered and filtered.

.. admonition:: Performance Hint
  :class: perf


    'Purifying' data does not result in any noticeable speed increase in the grouping, filtering
    and sorting of data. To the contrary, it's been my experience that Python's built-in generators outperform NumPy. This is
    mainly due to the cost of creating a recarray from the document store. Therefore, the only reason to 'purify' your data
    would be to make use of some of NumPy's functionality.

.. code-block:: python

  uv = Underverse()
  uv.load('data.sql')

  data = uv.data

  # creates a numpy recarray with 4 columns captured from the documents in the 'data' collection
  qd = data.purify('name', 'age', 'gender', 'country')

  # the new QuasiDead instance has limited functionality outside of it's code ``numpy.recarray``.
  # However, grouping and ordering can be achieved the same way as a document collection in Underverse.
  for country, citizens in qd.groupby('country'):
    print country, len(citizens)

  # numpy string array
  names = qd.name

.. seealso::

  Please look at the QuasiDead documentation for more information on what this unique class can be used for.

    """
    if HAS_NUMPY:
      return self.all().purify(*cols)#QuasiDead.from_dicts(self.division, *cols)
    else:
      raise Exception, "NumPy must be installed to use the 'purify' function."

  def __call__(self, *args):
    return self.find(*args)

  def limit(self, count):
    """
Returns a user-defined limited number of documents.

.. code-block:: python

  docs = uv.data.limit(50)

Or..

.. code-block:: python

  docs = uv.data.find(Document.age > 25).limit(50)

.. note::

  You can also chain the ``limit`` and ``skip`` functions together.

    """
    return self.all().limit(count)

  def skip(self, count):
    """
Returns all documents after the user-defined number have been skipped.

.. code-block:: python

  docs = uv.data.skip(50)

Or..

.. code-block:: python

  docs = uv.data.find(Document.age > 25).skip(50)

.. note::

  You can also chain the ``limit`` and ``skip`` functions together.

    """
    return self.all().skip(count)

  def put(self, key, value):
    """
This adds a KeyValue instance to the collection.


.. code-block:: python

  uv = Underverse()
  options = uv.options

  # Add key by using the ``put`` method
  options.put('key', 'value')


.. note::

  **Enumerations**

    Enumerations can be created easily by using the key-value pair functionality.

    .. code-block:: python

      uv = Underverse('data.db')
      options = uv.options

      # ENUMs can be created like this
      options.put('DEVMODES', {'TEST':0,'DEV':1,'PROD':2})

      # ENUMs can be retrieved like this
      DEVMODES = options.get('DEVMODES')
      print DEVMODES.PROD

      # Or like this.
      DEV = options.get('DEVMODES.DEV')

    """
    self.add(KeyValue(key, value))

  def get(self, key):
    """
This gets a document based on the key or UUID.

from underverse import Underverse, KeyValue

.. code-block:: python

  uv = Underverse()
  options = uv.options

  options.put('key', 5)

  option = options.get('key')
  print option.value

  # Key-Value pairs can also be used as other documents as well...
  for o in options:
    print o
    o.value2 = o.value * 2
    options.update(o)

  # Basically, the following line is doing the same thing as a KeyValue instance
  # n = NecRow({'uuid':'key', 'value':5})

    """
    if type(key) != str:
      raise TypeError, "Keys must be string values"

    value = self._cursor.execute('select data as "data [necro]" from %s where uuid = "%s"' % (self._name, key)).fetchone()
    if value is None:
      if '.' in key:
        attrs = key.split('.')
        value = self.get(attrs[0])
        if value is None:
          raise KeyError, "'%s' not found in document collection: '%s'" % (key, self._name)
        else:
          attrs.pop(0)
          for attr in attrs:
            try:
              value = getattr(value, attr)
            except Exception, e:
              raise KeyError, "Document does not have a key named '%s' " % (key)
          return value

      raise KeyError, "'%s' not found in document collection: '%s'" % (key, self._name)
    else:
      return value[0]

  def join(self, right, alias):
    """
This performs join operations on the current Verse.
    """
    return Join(self, right, alias)


class Underverse(object):
  """

This class is the core of the module. It provides the interface for either an in-memory or on-disk SQlite database. In-memory databases are noticably faster than files because of disk IO. However, this module provides functionality to both dump an in-memory database and load data into one.

Getting started is as easy as:

.. code-block:: python

  # importing the module
  from underverse import *

  # creates a connection to the ether
  uv = Underverse()

An on-disk data store can be created like so:

.. code-block:: python

  uv = Underverse('helion.db')

.. note::

  The extension doesn't matter, but traditionally SQlite databases are saved as either *.db* or *.sqlite*.

  """

  def __init__(self, filename=None):
    import datetime
    super(Underverse, self).__init__()

    if filename is None:
      self.connection = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_COLNAMES)
    else:
      self.connection = sqlite3.connect(filename, detect_types=sqlite3.PARSE_COLNAMES)

    if HAS_NUMPY:
      import numpy as np
      Underverse.register(np.ndarray, NumpyHandler)

  def __iter__(self):
    """
The Underverse can be iterated over to produce a list of 'Verses' or tables in the underlying SQlite data store.

.. code-block:: python

  from underverse import Underverse
  uv = Underverse('/path/to/on-disk/database.db')

  #Here's how to print a list of the tables
  for verse in uv:
    print verse

    """
    for name in self.connection.execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;'):
      yield name[0]

  def __getattr__(self, attr):
    """
New 'Verses' or tables can be created easily

.. code-block:: python

  verse = uv.helion

The above statement creates a two-column table in the SQlite data store.

    """
    if not attr in self.__dict__:
      return self.new(attr)

  def new(self, name):
    """
You can also create a table like this:

.. code-block:: python

  #this creates a new collection called 'data'
  verse = uv.new('data')

  #perhaps an easier and more elegant method is the following
  table2 = uv.table2

The above statements create a two-column table in the SQlite data store.

    """
    return Verse(self.connection, name)

  def close(self):
    """
Closes the database connection

.. code-block:: python

  uv.close()

    """
    self.connection.close()

  def dump(self, filename):
    """
Dumps the underverse to a .sql file which can be loaded in the future.

.. code-block:: python

  uv.dump("backup.sql")

    """
    with open(filename, 'w') as f:
      for line in self.connection.iterdump():
        f.write('%s\n' % line)

  def load(self, filename):
    """
Loads a persited underverse. Using this method to populate a collection is substantially faster than loading all the data using the *add* method.

.. code-block:: python

  uv.load("backup.sql")

    """
    with open(filename, 'r') as f:
     self.connection.cursor().executescript(f.read())

  @staticmethod
  def register(clazz, handler):
    """
    This registers a custom object handler with jsonpickle.

    Numpy arrays are handled by default, however, this is how it works.

    .. code-block:: python

      # import handlers
      from underverse import handlers
      import jsonpickle

      # create a custom handler for the encoding and decoding of NumPy arrays
      class NumpyHandler(jsonpickle.handlers.BaseHandler):

        # 'flatten' encodes an object into a JSON compatible object
        # In this example, the NumPy array is converted to a list before encoding
        def flatten(self, obj, data):
          data['__numpy__'] = obj.tolist()
          return data

        # 'restore' has to perform the opposite function as 'flatten'
        # Here, a list is converted back to a NumPy array
        def restore(self, obj):
          import numpy as np
          return np.array(obj['__numpy__'])

      # now the handler is registered
      Underverse.register(np.ndarray, NumpyHandler)

      # testing the encoding
      pickled = jsonpickle.encode(np.arange(5))
      print pickled               # {'py/object': 'numpy.ndarray', '__numpy__': [0, 1, 2, 3, 4]}

      # testing the decoding
      unpickled = jsonpickle.decode(pickled)
      print unpickled               # [0 1 2 3 4]

    """
    jsonpickle.handlers.registry.register(clazz, handler)

  @staticmethod
  def add_json_ext(encoder, decoder, clazz):
    """
Allows user to define custom JSON encoding and decoding.

.. note::

  This is an advanced feature. This is only useful if you want certain objects to be encoded in a custom way. Most users will probably never need to use this functionality.

  .. versionchanged: 0.3.5, 0.3.6, 0.4.0

  By using `jsonpickle <http://jsonpickle.github.com/index.html>`_ the object encoding should be much more **robust** and **simpler**.

.. code-block:: python

  # Here's a possible use case:
  # The following code transforms a NumPy array into a JSON list and back again
  # during the encoding and decoding process

  # the object is a numpy.ndarray; it's converted to a Python list before encoding
  numpy_encoder = lambda obj: obj.tolist()

  # the list is converted back to a numpy.ndarray object when decoding
  numpy_decoder = lambda obj: np.array(obj)

  Underverse.add_json_ext(numpy_encoder, numpy_decoder, np.ndarray)

**User Defined Encoders**:

    With the advent of version 0.4.0, the object encoding and decoding process 
    became much more simpler. Instead of a home-grown solution for object 
    encoding, I have opted to use *jsonpickle*.

    Now, encoders must be a callable object, such as a lambda or function.

**User Defined Decoders**:

    Decoders must also be a callable objects.

.. warning::
  
  The object encoding and decoding changed several times between versions 0.3.0 and 0.4.0. 
  This functionality should be stable now that a good solution has been found by using `jsonpickle <http://jsonpickle.github.com/index.html>`_.

    """

    if not callable(encoder):
      raise TypeError, "Encoders MUST be callable, either lambdas or functions"

    if not callable(decoder):
      raise TypeError, "Decoders MUST be callable, either lambdas or functions"
    
    class CustomHandler(jsonpickle.handlers.BaseHandler):
      """Abstract base class for handlers."""

      def flatten(self, obj, data):
        """Flatten obj into a json-friendly form."""
        data['value'] = encoder(obj)
        return data

      def restore(self, obj):
        """Restores the obj to type"""
        return decoder(obj['value'])
        # return np.array(obj['value'])

    Underverse.register(clazz, CustomHandler)


  @staticmethod
  def create_mappers(*_cls):
    """
    This function is now defunct as of v0.4.0.

    ``jsonpickle`` is now handling the object encoding and decoding. Python 
    objects should work automatically, however, if it doesn't use either ``add_json_ext`` OR the new ``register`` functionality.

    """
    import warnings
    warning = "\n\nPython objects should work automatically with v0.4.0. \nThis function call isn't needed any more. If you have any issues, use either \n'add_json_ext' OR the new 'register' functionality."
    # raise DeprecationWarning, warning
    warnings.warn(warning, Warning)

# This code will allow for the decoding of previously JSON-encoded Python objects.

# .. note::

#   This method is only required for the decoding of Python objects. Encoding
#   is handled automatically for all but the most demanding of circumstances.

# **Loading of Python objects** (script1.py)::

#   class Comment(object):
#     def __init__(self, text):
#       super(Comment, self).__init__()
#       self.text = text

#     def len(self):
#       return len(self.text)

#   class Message(object):
#     def __init__(self, x, y, z):
#       super(Message, self).__init__()
#       self.x = x
#       self.y = y
#       self.z = z
#       self.n = np.arange(5) # numpy array
#       self.m = Comment('Starting to come together')

#   class AnotherTestClass(object):
#     def __init__(self, text):
#       super(AnotherTestClass, self).__init__()
#       self.a = Comment(text)
#       self.msg = Message(2,3,4)
#       self.array = range(3)

#   uv = Underverse()

#   test = uv.test
#   test.add(AnotherTestClass('test #1'))

#   uv.dump('obj_testing.sql')


# .. note::

#   If you notice, none of the above classes require a 'base class' in order
#   to interact with Underverse. This allows the coder to use existing
#   classes without ANY changes as all...

#   This is one of many design elements which separates Underverse from all previous ORMs and ODMs.


# **Decoding of JSON into Python objects** (script2.py)::

#   from underverse import Underverse
#   from some_module import AnotherTestClass, Comment, Message

#   # creates the object mappers used to decode these three class
#   # notice that numpy.ndarray is not in the list, Underverse provides
#   # this capability as long as NumPy is installed
#   Underverse.create_mappers(AnotherTestClass, Comment, Message)

#   uv = Underverse()
#   uv.load('obj_testing.sql')

#   for r in self.uv.test:
#     print r.msg.m.text


# The code above was slightly modified from several test cases. Therefore,
# it should be guaranteed to work.. The example given obviously isn't
# a representation of an actual application, but rather an example of
# the existing support for nested class structures. The JSON is
# decoded flawlessly to reveal nearly exact duplicates of the objects
# that were encoded originally. The only difference are the memory
# spaces they reside in.

# .. note::

#   This method is **ONLY** required if the loading and reading of the
#   persisted documents happen in different scripts. Otherwise, Underverse
#   will remember the object classes from when they were loaded originally.

#     """
#     for c in _cls:
#       Underverse.create_encoder_decoder(c)
#     return True


if __name__ == '__main__':
  import time
  from test_data_gen import Person
  from model import Document

  uv = Underverse()
  # uv = Underverse(filename='testing.db')
  # uv.load('out.sql')

  verse = uv.mars
  verse.purge()
  # print verse.count()

  v = []
  start = time.clock()
  for i in range(5000):
    v.append(Person())
    if len(v) % 2500 == 0:
      verse.add(v)
      v = []
  verse.add(v)
  print "adding all: ", time.clock() - start


  # start = time.clock()
  # for i in range(50):
  #   verse.add(Person())
  # print "adding: ", time.clock() - start

  # print verse.cursor.execute("select data as 'data [necro]' from first where has('name', data)").fetchall()
  # start = time.clock()
  # c = 0
  # for v in verse:
  #   if 'F' in v.gender and v.age < 25:
  #     c += 1

  # print "hard-coded: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # for i in verse.find(('gender', ['eq', 'F']), ('age', ['lt', 25])):
    # c+= 1
  # print "predicate: ", time.clock() - start, c

  # results = verse.find(Document.name == "Max")

  # print len(verse)
  # print

  # print
  # start = time.clock()
  # c = 0
  # for i in verse.find_one(Document.name == "Max"):
    # print i
    # c+= 1
  # print "filter - eq: ", time.clock() - start, c

  # print
  # start = time.clock()
  # c = 0
  # for i in verse.glob('Max'):#.where(('name', ['search', 'Max'])):
  # for i in verse.glob(('Max',)):#.where(('name', ['search', 'Max'])):
    # print i
    # c+= 1
  # print "glob: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # for i in results.find(Document.skip(2)):
  #   # print i
  #   c+= 1
  # print "filter skip: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # for i in results[2:]:
  #   # print i
  #   c+= 1
  # print "filter skip slice: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # for i in results.find(Document.limit(5)):
  #   # print i
  #   c+= 1
  # print "filter limit: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # for i in results[:5]:
  #   # print i
  #   c+= 1
  # print "filter limit slice: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # # for i in verse.find(Document.name == "Max", Document.limskip(2, 5)):
  # for i in results >> stream.item[:5]:
  #   # print i
  #   c+= 1
  # # print "filter skip / limit: ", time.clock() - start, c
  # print "filter slice: ", time.clock() - start, c

  # start = time.clock()
  # c = 0
  # # for i in verse.find(Document.name == "Max", Document.limskip(2, 5)):
  # for i in results[:5]:
  #   # print i
  #   c+= 1
  # # print "filter skip / limit: ", time.clock() - start, c
  # print "filter slice 2: ", time.clock() - start, c
  # # print verse.find(Document.name == "Max")[]

  # start = time.clock()
  # c = 0
  # for i in results >> stream.drop(5) >> stream.take(5):
  #   # print i
  #   c+= 1
  # print "filter limit skip: ", time.clock() - start, c

  # print
  # start = time.clock()
  # c = 0
  # for i in verse.find(Document.name == "Max", Document.limskip(0, 5)):
  #   # print i
  #   c+= 1
  # print "filter limskip: ", time.clock() - start, c

  #Paging
  # p = 0
  # for page in verse >> stream.chop(500):
  #   print "Page %s" % p, len(page)
  #   p += 1

  #Paging
  p = 0
  for page in verse.paginate(500):
    print "Page %s" % p, len(page)
    p += 1

  exit()

  # print
  # start = time.clock()
  # c = verse.remove(verse.find(('name', ['search', 'John'])))
  # print "bulk remove: ", time.clock() - start, c

  start = time.clock()
  c = 0
  for i in verse.unique('name'):
    c+= 1
  print "uniq names: ", time.clock() - start, c

  def names(array):
    for a in array:
      yield a['name']

  start = time.clock()
  c = 0
  for i in verse.unique(names):
    c+= 1
  print "uniq names function: ", time.clock() - start, c

  start = time.clock()
  c = 0
  for i in verse.unique(Document.name):
    c+= 1
  print "uniq names filter: ", time.clock() - start, c

  start = time.clock()
  c = 0
  for i, j in verse.groupby('name'):
    c+= 1
    # print i, len(j)
  print "groupby names: ", time.clock() - start, c

  def names(array):
    for a in array:
      yield (a['name'], a['age'] // 10 * 10,), a

  start = time.clock()
  c = 0
  for i, j in verse.map(names):
    c+= 1
    # print i, len(j)
  print "map names: ", time.clock() - start, c

  # uv.dump('out.sql')
  # verse.purge()
  uv.close()
  print "Done."



