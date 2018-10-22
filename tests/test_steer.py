import unittest
from collections import OrderedDict
from pprint import pformat

from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.singleton import Singleton


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
    Singleton.reset(JobProperties)
    DATA = OrderedDict()
    DATA['graph'] = graph
    DATA['tree'] = tree
    DATA['algos'] = algos
    print(pformat(DATA))

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.steer = Steering('steer', loglevel="DEBUG")
        jobops = JobProperties()

        jobops.data['menu'] = self.DATA
        jobops.data['job'] = OrderedDict()
        jobops.data['job']['jobname'] = 'steertest'

    def tearDown(self):
        self.steer.finalize()
        Singleton.reset(JobProperties)

    def test_steer(self):
        self.steer.initialize()
        self.steer.book()
        self.steer.execute(b'payload')


if __name__ == "__main__":
    unittest.main()
