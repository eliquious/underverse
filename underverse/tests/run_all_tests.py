import unittest, os
from load_tests import LoadTestCase
from find_tests import FindTestCase
from mapreduce_tests import MapReduceTestCase
from kv_test import KeyValueTestCase
from underverse import Underverse
from underverse.model import Document
from test_data_gen import Person

if __name__ == '__main__':

  # if not os.path.exists('speed_test_smaller_obj.sql'):
  uv = Underverse()
  test = uv.test_obj
  test.purge()
  data = []
  for p in range(250):
    data.append(Person())
  test.add(data)
  uv.dump('speed_test_smaller_obj.sql')

  if not os.path.exists('speed_test_smaller.sql'):
    uv = Underverse()
    test = uv.test
    test.purge()
    data = []
    for p in range(250):
      data.append(Person().__dict__)
    test.add(data)
    uv.dump('speed_test_smaller.sql')

  suite1 = unittest.TestLoader().loadTestsFromTestCase(LoadTestCase)
  suite2 = unittest.TestLoader().loadTestsFromTestCase(FindTestCase)
  suite3 = unittest.TestLoader().loadTestsFromTestCase(MapReduceTestCase)
  suite4 = unittest.TestLoader().loadTestsFromTestCase(KeyValueTestCase)
  # unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([suite1, suite2, suite3]))
  unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([suite1, suite2, suite3, suite4]))
