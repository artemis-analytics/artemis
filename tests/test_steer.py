# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from collections import OrderedDict
from pprint import pformat
import logging

from artemis.core.gate import ArtemisGateSvc
from artemis.core.steering import Steering
from artemis.core.singleton import Singleton
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.meta.Directed_Graph import Directed_Graph, GraphMenu
from artemis.meta.Directed_Graph import Node as Node_pb2
from artemis.core.tree import Tree
from artemis_format.pymodels.configuration_pb2 import Configuration


class SteeringTestCase(unittest.TestCase):

    Singleton.reset(ArtemisGateSvc)

    def setUp(self):
        logging.getLogger().setLevel(logging.INFO)
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        # self.steer.finalize()
        Singleton.reset(ArtemisGateSvc)
        logging.getLogger().setLevel(logging.INFO)

    def test_from_msg(self):
        """
        Steering requires both the configuration and menu
        """
        testalgo = DummyAlgo1("dummy", myproperty="ptest", loglevel="INFO")

        seq1 = Node_pb2(["initial"], ("dummy", "dummy"), "seq1")

        dummyChain1 = Directed_Graph("dummy1")
        dummyChain1.add(seq1)
        dummyChain1.build()
        testmenu = GraphMenu("test")
        testmenu.add(dummyChain1)
        testmenu.build()

        msg = testmenu.to_msg()

        config = Configuration()
        algo = config.algos.add()
        algo.CopyFrom(testalgo.to_msg())

        jobops = ArtemisGateSvc()
        jobops.menu.CopyFrom(msg)
        jobops.config.CopyFrom(config)
        jobops.tree = Tree("dummy")

        a_steer = Steering("a_steer", loglevel="DEBUG")
        a_steer.initialize()
        a_steer.book()
        a_steer.execute(b"payload")
        # a_steer.finalize()
        # del jobops.data['menu']


if __name__ == "__main__":
    unittest.main()
