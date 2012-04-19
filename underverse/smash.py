"""
"Smash" is short-hand for Sum-Hash. This extension is essentially a multi-dimensional, 
hierarchical hash structure which stores all the distinct combinations across 
all dimensions. It also stores the number of times that combination has been
encountered, hence the name "sum-hash". Because it it hash-based, only the unique 
values are stored which makes for a simple compression algorithm. However, due to 
the large number of bytes it takes in Python to create dictionaries, you will 
likely see more memory usage.

This structure allows for O(1) performance for "put" and "get" operations. 
Multi-dimensional querying isn't quite O(1). However, due to the tree-like 
structure of the algorithm, as queries get more complicated (i.e. use more dimensions)
the algorithm continually speeds up, getting closer to O(1). Smash Tree's
intelligently filter data as more conditions are added to queries, effectively
making the search-space smaller and smaller.

Other benefits include blazing fast range searches and near instantaneous 
statistical analysis of any data stored in the hash tree. Because the counts of all 
the child nodes are stored in each parent node, the children can be sorted quickly
on frequency. Using these counts, histograms and cumulative distribution functions
can be calculated very fast, as well as quickly selecting top / bottom percentiles.
Min, max and median functions are also fast, because they loop through the unique 
objects and not the entire data set.

The reason these classes were added as an extension to Underverse and not simply as 
a separate module, is due to the shear usefulness of the MapReduce capability of Underverse.
Underverse has a mature MapReduce functionality, which can be used with pure Python dictionaries.

Similar to Underverse, this extension returns a dictionary-producing generator as 
the resultset for queries. Therefore, all functionality of Underverse is available
through the `SubVerse` class.

.. admonition:: Performance Hint
    :class: perf

    The hash tree algorithm has shown considerable performance capability over SQLite 
    in-memory databases. The tables below show insert and query speeds for various 
    analysis modules I have written.
    
    **Data**

    The insert table shows the times it took to insert 50k rows of data with 5 columns.
    The data consisted of a list of people with names, gender, age, college and 
    number of friends.

    The query table shows the time it took to find the number of rows which matched:
        
        * gender:  "M"
        * college: 3
        * name:    "Max"


    .. raw:: html

        <br \>
        <div style="width:90%" id="main">
          <table class="ctable" id="ins_speed_comparison">
              <thead>
                <tr>
                    <td>Module</td>
                    <td>Execution Time (Rows / Sec)</td>
                    <td>Built on top of</td>
                </tr>
              </thead>
            <tbody>
                <tr>
                  <td>Smash Tree</td>
                  <td>0.947s (52829.0)</td>
                  <td>Python dict</td>
                </tr>
                <tr>
                  <td>Bops</td>
                  <td>1.585s (31537.0)</td>
                  <td>NumPy</td>
                </tr>
                <tr>
                  <td>Underverse</td>
                  <td>9.289s (5382.6)</td>
                  <td>SQLite (in-memory)</td>
                </tr>
            </tbody>
          </table>
        </div>
        <br \>
        <div style="width:90%" id="main">
          <table class="ctable" id="query_speed_comparison">
              <thead>
                <tr>
                    <td>Module</td>
                    <td>Execution Time</td>
                    <td>Built on top of</td>
                </tr>
              </thead>
            <tbody>
                <tr>
                  <td>Smash Tree</td>
                  <td>0.002s</td>
                  <td>Python dict</td>
                </tr>
                <tr>
                  <td>Bops</td>
                  <td>0.008s</td>
                  <td>NumPy</td>
                </tr>
                <tr>
                  <td>Underverse</td>
                  <td>17.073s</td>
                  <td>SQLite (in-memory)</td>
                </tr>
            </tbody>
          </table>
        </div>
        <br \>

"""

from underverse import NecRow, SubVerse


class Item(object):
    """This class records the unique value and the number of occurrences."""
    def __init__(self, attr):
        super(Item, self).__init__()
        self.attr = attr
        self.count = 1

    def incr(self):
        """Increases the count of this item"""
        self.count += 1
        return self

    def decr(self):
        """Decreases the count of this item"""
        if self.count > 0:
            self.count -= 1
        return self

    def __cmp__(self, other):
        if self.attr < other:
            return -1
        elif self.attr == other:
            return 0
        else:
            return 1

class Node(Item):
    """All nodes in the hash tree are Nodes."""
    def __init__(self, attr):
        super(Node, self).__init__(attr)
        self.children = {}

    def is_leaf(self):
        """Determines if the node has any children"""
        return len(self.children)== 0

    def items(self, unique=True):
        """
        Returns a sorted list of all the children nodes and their values.

        This function acts exactly like the `dict.items()` method.
        """
        for key, child in sorted(self.children.items()):
            if child.is_leaf() and unique:
                for i in xrange(child.count):
                    yield key, child
            else:
                yield key, child

    def keys(self):
        """
        Returns all the unique keys of this node's children.
        """
        return sorted(self.children.keys())

    def values(self):
        """
        Returns a sorted list of all the child nodes
        """
        return sorted(self.children.values())

    def insert(self, attr, node):
        """Adds a child to node"""
        self.children[attr] = node
        return self.children[attr]

class Tree(Node):
    """Base class for the module."""
    def __init__(self, name="root", value="root", names=[]):
        super(Tree, self).__init__(name)
        self.names = names
        self.name = name
        self.value = value

    def put(self, **kwargs):
        """
        This method inserts a multi-dimensional key-value pair into the data store.
        """
        current = self
        self.count += 1

        # add attribute/dimension to tree if attr hasn't been seen before
        for key in kwargs:
            if not key in self.names:
                self.names.append(key)

        # loop through all dimensions
        for i, name in enumerate(self.names):

            # if name in new data
            if name in kwargs:

                # if value of current dimension exists increment, otherwise create it
                if kwargs[name] in current.children:
                    current = current.children[kwargs[name]].incr()
                else:
                    current = current.insert(kwargs[name], Tree(name, kwargs[name], names=self.names[i:len(self.names)]))
                    # current = current.children[kwargs[name]].incr()
            else:
                if "None" in current.children:
                    current = current.children["None"].incr()
                else:
                    current = current.insert("None", Tree(name, "None", names=self.names[i:len(self.names)]))
                    # current = current.children["None"].incr()

    def _is_filtered(self, conditions):
        if not conditions:
            return False
        
        if self.name in conditions:
            if hasattr(conditions[self.name], "__call__"):
                return not conditions[self.name](self.value)
            else:
                return not self.value == conditions[self.name]
        else:
            return False

    def query(self, unique=True, objectify=False, filters=None):
        """
        This method is used to filter data. It produces a generator of dictionaries
        matching the filters.
        """
        for child in self:
            if not child._is_filtered(filters):
                if child.is_leaf():
                    if unique:
                        for item in xrange(child.count):
                            yield {child.name: child.value}
                    else:
                        yield {child.name: child.value}
                else:
                    for grand in child.query(unique, objectify, filters):
                        if objectify:
                            n = NecRow(grand)
                            yield n
                        else:
                            grand[self.name] = self.value
                            yield grand
    
    def query_list(self, unique=True, filters=None):
        """
        This method is used to filter data. It produces a generator of lists
        matching the filters.
        """
        for child in self:
            if not child._is_filtered(filters):
                if child.is_leaf():
                    if unique:
                        for item in xrange(child.count):
                            yield [self.value, child.value]
                    else:
                        yield [self.value, child.value]
                else:
                
                    for grand in child.query_list(unique, filters):
                        if not self.value == "root":
                            grand.insert(0, self.value)
                        yield grand

    def paths(self):
        """
        Yields a list of all paths available in the tree data structure.
        """
        for path in self.query():
            yield path

    def filter_children(self, where, unique=True):
        """
        This method filters the children of a given node by imposing the filters on the list of children.
        Therefore, only the children matching the criteria are yielded.
        """
        for name, child in self.items(unique):
            if not where:
                if not unique:
                    yield name, child
                else:
                    if child.is_leaf():
                        for uniq in xrange(child.count):
                            yield name, child
                    else:
                        yield name, child
            else:
                if not child._is_filtered({child.name:where}):
                    if not unique:
                        yield name, child
                    else:
                        if child.is_leaf():
                            for uniq in xrange(child.count):
                                yield name, child
                        else:
                            yield name, child

    def get(self, **kwargs):
        """
        This method returns a single node which matches the `kwargs` exactly. This
        is the absolute fastest way to get data from the data structure. However, 
        it cannot be used in all situations.
        """
        current = self
        
        for name in self.names:
            if name in kwargs:
                if not kwargs[name] in current.children:
                    if not "None" in current.children:
                        return None
                    else:
                        current = current.children["None"]
                else:
                    current = current.children[kwargs[name]]
            else:
                break
        return current

    def count(self, **kwargs):
        """
        Returns the count of a given child node.
        """
        cnt = self.get(**kwargs)
        if cnt is not None:
            return cnt.count
        return None

    def count_all_unique_children(self):
        count = len(self.keys())
        for key, value in self.items():
            count += value.count_all_unique_children()
        return count

    def sum_all_children(self):
        count = len(self.keys())
        if count == 0:
            count += self.count
        for key, value in self.items():
            count += value.sum_all_children()
        return count
    
    def print_all_children(self, indent=0):
        if indent == 0:
            print self
        count = len(self.keys())
        for key, value in self.items():
            print " " * (indent), "", value
            count += value.print_all_children(indent + 2)
        return count

    def iterate(self, unique=True):
        """
        Iterates over all the children
        """
        for child in self:
            if child.is_leaf() and unique:
                for uniq in xrange(child.count):
                    yield child
            else:
                yield child


    def max(self, filter=None):
        """
        Sorts the children and returns the first one.
        """
        if self.is_leaf():
            return None
        else:
            if not filter:
                return self.children[max(self.keys())]
            else:
                for child in self.order_by_count(False, filter):
                    return child

    def min(self, filter=None):
        """
        Sorts the children and returns the last one.
        """
        if self.is_leaf():
            return None
        else:
            if not filter:
                return self.children[min(self.keys())]
            else:
                for child in self.order_by_count(True, filter):
                    return child

    def counts(self, filter=None):
        """
        Returns all the children (and their counts) not filtered by the given conditions.
        """
        for name, child in self.items():
            if not filter:
                yield child, child.count
            else:
                if not child._is_filtered({child.name:filter}):
                    yield child, child.count

    def cdf(self, filter=None):
        """
        Returns a list of children sorted by counts in ASC order.
        """
        _sum = 0
        for child in self.order_by_count(True, filter):
            _sum += child.count
            yield child, _sum

    def ratio(self, filter=None):
        """
        Returns the ratio of children vs. their counts.
        """
        if not filter:
            return len(self.children) / float(self.count)
        else:
            count = 0
            _sum = 0
            for child in self:
                if not child._is_filtered({child.name:filter}):
                    count += 1
                    _sum += child.count
            return count / _sum

    def median(self, filter=None):
        """
        Returns the median child.
        """
        total = self.count
        _sum = 0
        for value, counts in self.counts(filter):
            _sum += counts
            if total / 2.0 < _sum:
                return value

    def order_by_count(self, asc=True, filter=None):
        """
        Sorts the children by their counts.
        """
        if asc:
            k = lambda value: value.count
        else:
            k = lambda value: -value.count
        for value in sorted(self.values(), key=k):
            if not filter:
                yield value
            else:
                if not value._is_filtered({value.name:filter}):
                    yield value

    def top(self, num, filter=None):
        """
        Returns the top X most frequent children
        """
        if num < 1:
            raise ValueError("Number must be larger than 0.")
        count = 0
        for child in self.order_by_count(False, filter):
            if count < num:
                yield child
            else:
                break
            count += 1

    def bottom(self, num, filter=None):
        """
        Returns the top X number of least frequent children
        """
        if num < 1:
            raise ValueError("Number must be larger than 0.")
        count = 0
        for child in self.order_by_count(True, filter):
            if count < num:
                yield child
            else:
                break
            count += 1

    def __contains__(self, value):
        return value in self.children

    def __iter__(self):
        return iter(self.children.values())

    def __len__(self):
        return len(self.children)

    def __repr__(self):
        # return "<Node: %s=%s (%s:%s)>" % (self.attr, self.value, len(self.children), self.count)
        return "<%s: %s (%s:%s)>" % (self.attr, self.value, len(self.children), self.count)

if __name__ == '__main__':
    import time
    
    bt = Tree(names=["gender", "college", "name", "age", "friends"])
    # bt = Tree()

    data = []
    start = time.clock()
    LINECOUNT = 50000
    with open('people.csv', 'r') as fh:
        fh.readline()
        count = 0
        for line in fh:
            if count < LINECOUNT:
                words =line.strip("\n").split(",")
                bt.put(name=words[0], gender=words[1], age=int(words[2]), college=int(words[3]), friends=int(words[4]))
                data.append(words)
            else:
                break
            count += 1
            if count % 5000 == 0:
                print "\rLoading... (%0.2f%%)" % (count / float(LINECOUNT) * 100),
        print

    end = time.clock()
    print end - start, (LINECOUNT * 5) / (end - start)
    print

    def upperclass(value):
        return int(value) == 3

    # print repr(bt)
    # for x in bt.paths_hash(query={"gender":"M", "college":upperclass}):
    start = time.clock()
    count = 0
    # for x in bt.query(gender="M", college=3):
    for x in bt.query(filters={"gender":"M", "college":3, "name":"Max"}):
        count += 1
        # print x
    end = time.clock()
    print "paths:", end - start, count
    
    # print
    start = time.clock()
    count = 0
    for words in data:
        if words[1] == "M":
            if upperclass(words[3]) and words[0] == "Max":
                count += 1
                # print words
    end = time.clock()
    print "list:", end - start, count

    # print 
    import time
    start = time.clock()
    count = 0
    for x in bt.get(gender="M", college=3):
        if x.value == "Max":
            for y in x.iterate():
            # print y
                for z in y.iterate():
                    count += 1
        # print x
    end = time.clock()
    print "iterate:", end - start, count
    
    start = time.clock()
    count = 0
    filters = {"gender":"M", "college":3, "name":"Max"}
    # root = bt.get(gender="M", college="3")

    for name, person in bt.get(gender="M", college=3).filter_children("Max"):
        for age, a in person.filter_children(lambda val: int(val) > 75):

            for friends, f in a.items():
                count += 1

    end = time.clock()
    print "filter_children:", end - start, count


    start = time.clock()
    count = bt.get(gender="M", college=3, name="Max").count
    end = time.clock()
    print "cached count:", end - start, count

    count = 0
    start = time.clock()
    for child in bt.query_list(filters=filters):
        # print child
        count += 1
    # print count
    end = time.clock()
    print "query list:", end - start, count


    # print
    # node = bt.get(gender="M", college="3")
    # print node.min()
    # print node.median()
    # print node.max()

    # print 
    # print node.count
    # _sum = 0
    # for value, counts in node.counts():
    #     print value, _sum, _sum + counts
    #     _sum += counts
    
    # def name_filter(value):
    #     return str(value)[0] == "M"

    # print
    # for child in node.order_by_count(False, filter=name_filter):
    #     print child

    # print
    # for child in node.top(5):
    #     print child
    
    # print 
    # for child, cdf in node.cdf():
        # print child, cdf
    
    # print 
    # avg = 0
    # for age, count in bt.get(gender="M", college="3", name="Max").counts():
    #     # print age.value, count
    #     avg += (int(age.value) * count)
        
    # print
    # print avg / float(bt.get(gender="M", college="3", name="Max").count)
    # print bt

    # print bt.sum_all_children()

#     print
#     start = time.clock()
#     count = 1000
#     for x in range(count):
#         bt.get(gender="M", college=3).count
# #         print x
#     end = time.clock()
#     print end - start, count / (end - start), count
    print "Done."


