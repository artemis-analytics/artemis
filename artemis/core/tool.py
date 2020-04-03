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
ToolBase class, right now not much different than AlgoBase
Does not support histograms, timers, etc...
These should be in the Algo which calls the tool

Tools can be registered in a ToolSvc, and lookup via name
through the Algo via a property with name of tool

"""
import importlib
from pprint import pformat

from artemis.logger import Logger
from artemis.core.properties import Properties
from artemis_format.pymodels.configuration_pb2 import Module as Tool_pb

from artemis.core.abcalgo import AbcAlgoBase


class ToolBase(metaclass=AbcAlgoBase):
    def __init__(self, name, **kwargs):
        """
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc...
        can be done through the classmethods

        Can we use staticmethods in artemis to make uniform
        formatting of info, debug, warn, error?
        """
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug("__init__ ToolBase")
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

    @property
    def name(self):
        """
        Tool name
        """
        return self.__name

    def to_msg(self):
        message = Tool_pb()
        message.name = self.name
        message.klass = self.__class__.__name__
        message.module = self.__module__
        message.properties.CopyFrom(self.properties.to_msg())
        return message

    @staticmethod
    def from_msg(logger, msg):
        logger.info("Loading Tool from msg %s", msg.name)
        try:
            module = importlib.import_module(msg.module)
        except ImportError:
            logger.error("Unable to load module %s", msg.module)
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, msg.klass)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, msg.klass))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        properties = Properties.from_msg(msg.properties)
        logger.debug(pformat(properties))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if "loglevel" not in properties:
            properties["loglevel"] = logger.getEffectiveLevel()

        try:
            instance = class_(msg.name, **properties)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, "properties"))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def lock(self):
        """
        Lock all properties for algorithm
        """
        self.properties.lock = True

    def initialize(self):
        """
        Framework initialize
        """
        pass

    def execute(self, payload):
        """
        Tool can take any input defined by the user
        Ideally, most tools accept an Arrow Array or batch
        """
        pass
