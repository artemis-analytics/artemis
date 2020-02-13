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

"""
Example algorithm class.
"""
import sys
import logging
import random

from artemis.core.algo import AlgoBase


class DummyAlgo1(AlgoBase):
    """Dummy Algorithm use a template for writing User-defined algorithms UDA
    
    Attributes
    ----------
        __logger : Logger
            logging managed by framework

        gate : ArtemisGateSvc
            access to histograms, metadata, and stores

    Parameters
    ----------
        name : str
            Name of algorithm as configured in the metadata

    Other Parameters
    ----------------
        
    Returns
    -------

    Exceptions
    ----------

    Examples
    --------
    """
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ DummyAlgo1' % self.name)

    def initialize(self):
        """Additional initialization as defined by the user can be done here.
        """
        self.__logger.info('%s: initialize DummyAlgo1' % self.name)
        self.__logger.info('%s: property %s' %
                           (self.name, self.properties.myproperty))
        self.__logger.info('%s: Initialized DummyAlgo1' % self.name)

    def book(self):
        """
        Book all histograms that will be used in this algorithm. 
        Histogram names will be modified with the algorithm name."""

        # Access histograms books via the gate service
        self.gate.hbook.book(self.name, "testh1", range(10))

    def execute(self, element):
        """
        Execute operations on data that is associated with a node in the process graph.
        
        Parameters
        ----------
            element : Element
                Element provides a reference to data that is managed in a common memory store (key-value). 
                
        """
        if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                self.__logger.isEnabledFor(logging.DEBUG)):
            
            # Retrieve data from the datastore via the element for this node
            raw_ = element.get_data()

            # Prevent excessive formating calls when not required
            # Note that we can indepdently change the logging level
            # for algo loggers and root logger
            # Use string interpolation to prevent excessive format calls
            self.__logger.debug('%s: execute ' % self.name)
            # Check logging level if formatting requiered
            self.__logger.debug('{}: execute: payload {}'.
                                format(self.name, sys.getsizeof(payload)))

            # Fill a histogram 
            self.gate.hbook.fill(self.name, "testh1", random.randint(0, 10))

            # Add data to the datastore for this node
            element.add_data(raw_)

    def finalize(self):
        """Any postprocessing at the end of the job, such as creating summary information is done here."""

        pass
