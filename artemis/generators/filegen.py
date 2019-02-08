#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Class for generating lists of input files from OS
"""
import pathlib

from artemis.generators.common import GeneratorBase


class FileGenerator(GeneratorBase):
    '''
    Use a path and globbing pattern
    return a generator over the files
    '''
    def __init__(self, name, **kwargs):

        self._defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            self._defaults[key] = kwargs[key]
        # Set the properties with the full configuration
        super().__init__(name, **self._defaults)

        self._path = self.properties.path
        self._glob = self.properties.glob
        self._seed = self.properties.seed
        self.__logger.info("Path %s", self._path)
        self.__logger.info("Glob %s", self._glob)

    def _set_defaults(self):
        defaults = {'seed': 42}
        return defaults

    def generate(self):
        return pathlib.Path(self._path).glob(self._glob)
