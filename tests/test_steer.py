import unittest
from collections import OrderedDict
from pprint import pformat

from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.singleton import Singleton
from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1


class SteeringTestCase(unittest.TestCase):

    testalgo = DummyAlgo1('dummy', myproperty='ptest', loglevel='INFO')

    seq1 = Sequence(["initial"], (testalgo, testalgo), "seq1")

    dummyChain1 = Chain("dummy1")
    dummyChain1.add(seq1)
    testmenu = Menu("test")
    testmenu.add(dummyChain1)
    testmenu.generate()
    
    msg = testmenu.to_msg()

    Singleton.reset(JobProperties)
    DATA = OrderedDict()
    DATA['protomsg'] = msg
    print(pformat(DATA))

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.steer = Steering('steer', loglevel="DEBUG")

    def tearDown(self):
        self.steer.finalize()
        Singleton.reset(JobProperties)
    
    def test_from_msg(self):
        jobops = JobProperties()
        jobops.meta.config.menu.CopyFrom(self.DATA['protomsg'])
        a_steer = Steering('a_steer', loglevel="DEBUG")
        a_steer.initialize()
        a_steer.book()
        a_steer.execute(b'payload')
        #del jobops.data['menu']


if __name__ == "__main__":
    unittest.main()
