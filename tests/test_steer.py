import unittest
from collections import OrderedDict
from pprint import pformat

from artemis.core.properties import JobProperties
from artemis.core.steering import Steering

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

    DATA = OrderedDict()
    DATA['graph'] = graph
    DATA['tree'] = tree
    DATA['algos'] = algos
    print(pformat(DATA))

    jobops = JobProperties()
    jobops.data['menu'] = DATA
    jobops.data['job'] = OrderedDict()
    jobops.data['job']['jobname'] = 'steertest'

    def setUp(self):
        self.steer = Steering('steer')

    def tearDown(self):
        pass

    def test_steer(self):
        self.steer.initialize()
        self.steer.execute("payload")


if __name__ == "__main__":
    unittest.main()


