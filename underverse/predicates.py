import re
from operator import attrgetter

## TODO:
## 
## Change process to yield, not return
##

#+ <, <=, >, >=
# $all
#+ $exists
# $mod
#+ $ne
#+ $in
#+ $nin
# $nor
# $or
# $and
# $size
#+ $type
#+ Regular Expressions
#+ Value in an Array
# $elemMatch

#+ skip
#+ limit

__all__ = ["Predicate", "AND", "OR"]

class UnknownPredicate(Exception):
  # """UnknownPredicate: raised when the predicate (conditional filter) is not understood"""
  def __init__(self, value):
    super(UnknownPredicate, self).__init__()
    self.value = value
  
  def __str__(self):
    yield repr(self.value)

class Predicate(object):
  # """docstring for Predicate"""
  def __init__(self):
    super(Predicate, self).__init__()
    
  # conditional Predicates  
  @classmethod
  def exists(Predicate, attr):
#     """
# Checks to see if a document has an attribute
#     """
    return lambda x: True if attr in x else False
  
  @classmethod
  def lt(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] < value

# .. code-block:: python

#   verse.find(Document.age < 25)

#     """
    return lambda x: True if getattr(x, attr) < value else False
  
  @classmethod
  def lte(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] <= value

# .. code-block:: python

#   verse.find(Document.age <= 25)

#     """
    return lambda x: True if getattr(x, attr) <= value else False
  
  @classmethod
  def gt(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] > value

# .. code-block:: python

#   verse.find(Document.age > 25)

#     """
    return lambda x: True if getattr(x, attr) > value else False
  
  @classmethod
  def gte(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] >= value

# .. code-block:: python

#   verse.find(Document.age >= 25)

#     """
    return lambda x: True if getattr(x, attr) >= value else False
  
  @classmethod
  def eq(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] >= value

# .. code-block:: python

#   verse.find(Document.age >= 25)

#     """
    return lambda x: True if getattr(x, attr) == value else False
  
  @classmethod
  def ne(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] >= value

# .. code-block:: python

#   verse.find(Document.age >= 25)

#     """
    return lambda x: True if getattr(x, attr) != value else False
  
  @classmethod
  def type_(Predicate, attr, value):
#     """
# Finds all documents where type(doc[attr]) == value

# .. code-block:: python

#   verse.find(Document.age.type() >= 25)

#     """
    yield lambda x: True if type(getattr(x, attr)) is value else False
  
  @classmethod
  def in_(Predicate, attr, value):
#     """
# Finds all documents where doc[attr] in value

# .. code-block:: python

#   verse.find(Document.name.in(['Max', 'Tamara'])

#     """
    return lambda x: True if getattr(x, attr) in value else False

  @classmethod
  def nin(Predicate, attr, value):
#     """
# Finds all documents where not doc[attr] in value

# .. code-block:: python

#   verse.find(Document.name.nin(['Max', 'Tamara'])

#     """
    return lambda x: True if not getattr(x, attr) in value else False

  @classmethod
  def match(Predicate, attr, value):
#     """
# Finds all documents where value is found in doc[attr]

# .. code-block:: python

#   #False if re.compile(value).match(doc[attr]) else True
#   verse.find(Document.name.match('J') )

#     """
    return lambda x: True if re.compile(value).match(getattr(x, attr)) else False

  @classmethod
  def search(Predicate, attr, value):
#     """
# Finds all documents where value is found in doc[attr]

# .. code-block:: python

#   #False if re.compile(value).search(doc[attr]) else True
#   verse.find(Document.name.search('J') )

#     """
    return lambda x: True if re.compile(value).search(getattr(x, attr)) else False

  @classmethod
  def nmatch(Predicate, attr, value):
#     """
# Finds all documents where value is not found in doc[attr]

# .. code-block:: python

#   #False if re.compile(value).match(doc[attr]) else True
#   verse.find(Document.name.nmatch('J') )

#     """
    return lambda x: False if re.compile(value).match(getattr(x, attr)) else True

  @classmethod
  def nsearch(Predicate, attr, value):
#     """
# Finds all documents where value is not found in doc[attr]

# .. code-block:: python

#   #False if re.compile(value).search(doc[attr]) else True
#   verse.find(Document.name.nsearch('J') )

#     """
    return lambda x: False if re.compile(value).search(getattr(x, attr)) else True

  @classmethod
  def len(Predicate, attr, value):
#     """
# Finds all documents where len(doc[attr]) == value

# .. code-block:: python

#   #finds all people with 6 letter names..
#   verse.find(Document.name.len(6))

#     """
    return lambda x: True if len(getattr(x, attr)) == value else False
    # return lambda x: len(getattr(x, attr)) if hasattr(x, '__len__') else 0

  @classmethod
  def btw(Predicate, attr, left, right):
#     """
# Finds all documents where left < doc[attr] < right

# .. code-block:: python

#   verse.find(Document.age.btw(25, 35))

#     """
    return lambda x: True if left < getattr(x, attr) < right else False

  @classmethod
  def udp(Predicate, attr, function, *args, **kwargs):
#     """

# User Defined Predicate

#     """
    return lambda x: True if function(getattr(x, attr), *args, **kwargs) else False

  @classmethod
  def udf(self, function, *args, **kwargs):
#     """
# Calls a function to determine which rows are found

# .. note::
  
#   Skip, Limit and Order By use this functionality.

#     """
    return lambda x: function(x, *args, **kwargs)

  @classmethod
  def orderby(Predicate, *args):
#     """
# Orders all documents by the attributes given

# .. code-block:: python

#   verse.find(Document.orderby('name', 'age'))

#     """
    def sort(data, attrs):
      attrs = list(attrs)
      attrs.reverse()
      for attr in attrs:
        if type(attr) != str:
          raise TypeError, "Orderby arguments must be strings"
        if attr.startswith('-'):
          data = sorted(data, key=attrgetter(attr[1:]), reverse=True)
        else:
          data = sorted(data, key=attrgetter(attr))
      return data
    
    return lambda x: sort(x, args)
    # return lambda x: tuple([getattr(x, arg) for arg in args])

class OR(object):
  """
  This class provides for the logical *OR-ing* of conditions.

  .. code-block:: python

    from underverse.model import Document as D

    # SELECT * FROM test WHERE 
    #   ((age BETWEEN 30 AND 35) OR (age BETWEEN 60 AND 65)) AND 
    #   (name = 'Billy' OR name = 'Zaphod'));
    r = uv.users.find(OR(D.age.btw(30, 35), D.age.btw(60, 65)), OR(D.name == 'Billy', D.name == 'Zaphod'))

  The code above selects all 'users' who are 30-35 OR 60-65 years old AND whose names are either 'Billy' OR 'Zaphod'.
  The name filter can be simplified by using ``D.name.in_(['Billy', 'Zaphod'])``

  """
  def __init__(self, *filters):
    super(OR, self).__init__()
    self.filters = filters

  def __call__(self, data):
    def match(result):
      if type(result) == bool:
        return result
      elif hasattr(result, '__iter__'):
        return len(list(result)) > 0
      else:
        return False
    
    for d in data:
      tmp = False
      for _filter in self.filters:
        if hasattr(_filter, '__predicate__'):
          tmp = _filter.predicate(d)
        elif type(_filter) in [AND, OR]:
          tmp = match(_filter([d]))
        elif callable(_filter):
          tmp = match(_filter(d))
        else:
          raise TypeError, "Filter given isn't recognized"
        if tmp == True:
          yield d
          break

  def __str__(self):
    return '('+' OR '.join([str(f) for f in self.filters])+')'

class AND(object):
  """
  This provides for the logical *AND-ing* of conditions. This is the default behavior of Underverse.

  However, this can be used in conjunction with the OR to perform more powerful queries.

  .. code-block:: python

    # SELECT * FROM test WHERE 
    #   (name = 'Billy' AND age = 31) OR 
    #   (name = 'Zaphod' AND (age between 60 and 65)));
    r = test.find(OR(AND(Document.name == 'Zaphod', Document.age.btw(60, 65)), AND(Document.name == 'Billy', Document.age == 31)))

  .. note:: 
    
    Any conditions separated by a comma in the ``find`` functions are AND-ed together.

  """
  def __init__(self, *filters):
    super(AND, self).__init__()
    self.filters = filters

  def __call__(self, data):
    def match(result):
      if type(result) == bool:
        return result
      elif hasattr(result, '__iter__'):
        return len(list(result)) > 0
      else:
        return False
        
    for d in data:
      tmp = True
      for _filter in self.filters:
        # print _filter
        if hasattr(_filter, '__predicate__'):
          tmp = _filter.predicate(d)
        elif type(_filter) in [AND, OR]:
          tmp = match(_filter([d]))
        elif callable(_filter):
          tmp = match(_filter(d))
        else:
          raise TypeError, "Filter given isn't recognized"
        if not tmp:
          break
      if tmp:
        yield d

  def __str__(self):
    return '('+' AND '.join([str(f) for f in self.filters])+')'

  # @classmethod
  # def new(Predicate, function):
  #   """
  #   add a list of udf Predicates
  #   """
  #   raise NotImplementedError

