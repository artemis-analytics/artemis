#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
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
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import coverage
import unittest
import logging
import tempfile

from artemis.artemis import Artemis, ArtemisFactory
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.gate import ArtemisGateSvc
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)

    """ 
    def test_proto(self):
        cov = coverage.Coverage()
        cov.start()
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory('csvgen')
            msgmenu = mb.build()
            config = JobConfigFactory('csvgen',msgmenu,
                                      jobname='arrowproto',
                                      output_repo=dirpath)
            config.configure()
            msg = config.job_config
            job = JobInfo_pb()
            job.name = 'arrowproto'
            job.job_id = 'example'
            job.output.repo = dirpath
            job.config.CopyFrom(msg)
            #job.job_id = str(uuid.uuid4())
            print(job)
            bow = ArtemisFactory(job, 'INFO')
            bow.control()
            
            cov.stop()
            cov.save()
        """


if __name__ == "__main__":
    unittest.main()
