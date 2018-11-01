import unittest
from collections import OrderedDict
from pprint import pformat

from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.singleton import Singleton
from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1


class SteeringTestCase(unittest.TestCase):

    graph = OrderedDict([("initial", [
                "iorequest"
            ]),
            ("seq1", [
                "dummy"
            ]),
            ("seq2", [
                "dummy"
            ]),
            ("seq3", [
                "dummy"
            ]),
            ("seq4", [
                "dummy"
            ])]
        )
    tree = OrderedDict([
                ("initial", []),
                ("seq1", [
                    "initial"
                ]),
                ("seq2", [
                    "initial"
                ]),
                ("seq3", [
                    "seq1",
                    "seq2"
                ]),
                ("seq4", [
                    "seq3"
                ])
            ])
    algos = OrderedDict(
                [("iorequest", {}),
                 ("dummy", {
                            "name": "dummy",
                            "class": "DummyAlgo1",
                            "module": "artemis.algorithms.dummyalgo",
                            "properties": {
                                "myproperty": "ptest",
                                "loglevel": "DEBUG"
                                }
                            }
                  )])

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
    DATA['graph'] = graph
    DATA['tree'] = tree
    DATA['algos'] = algos
    DATA['protomsg'] = msg
    print(pformat(DATA))

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.steer = Steering('steer', loglevel="DEBUG")
        jobops = JobProperties()

        #jobops.data['menu'] = self.DATA
        jobops.data['job'] = OrderedDict()
        jobops.data['job']['jobname'] = 'steertest'

    def tearDown(self):
        self.steer.finalize()
        Singleton.reset(JobProperties)
    
    def test_from_msg(self):
        jobops = JobProperties()
        jobops.data['menu'] = {'protomsg': self.DATA['protomsg']}
        a_steer = Steering('a_steer', loglevel="DEBUG")
        a_steer.initialize()
        a_steer.book()
        a_steer.execute(b'payload')
        del jobops.data['menu']
    
    def test_from_dict(self):
        jobops = JobProperties()
        jobops.data['menu'] = {'graph': self.DATA['graph'],
                               'tree': self.DATA['tree'],
                               'algos': self.DATA['algos']}
        self.steer.initialize()
        self.steer.book()
        self.steer.execute(b'payload')
        del jobops.data['menu']
    

if __name__ == "__main__":
    unittest.main()
