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
Class for generating lists of input files from OS
"""
from artemis_base.utils.decorators import iterable
from artemis.generators.common import GeneratorBase


@iterable
class FileGenOptions:
    nsamples = 1
    # seed = 42


class FileGenerator(GeneratorBase):
    """
    Use a path and globbing pattern
    return a generator over the files
    """

    def __init__(self, name, **kwargs):

        options = dict(FileGenOptions())
        options.update(kwargs)

        super().__init__(name, **options)

        # self._path = self.properties.path
        self._glob = self.properties.glob
        # self._seed = self.properties.seed
        self._nsamples = self.properties.nsamples
        self._batch_iter = None
        # self._batch_iter = pathlib.Path(self._path).glob(self._glob)
        # self.__logger.info("Path %s", self._path)
        self.__logger.info("Glob %s", self._glob)
        # self.__logger.info("Seed %s", self._seed)
        self.__logger.info("Samples %s", self._nsamples)

    def initialize(self):
        ids = []
        for obj in self.gate.store.list(
            prefix=self.gate.meta.parentset_id, suffix=self._glob
        ):
            ids.append(obj.uuid)
        self._batch_iter = iter(ids)

    def reset(self):
        # self._batch_iter = pathlib.Path(self._path).glob(self._glob)
        ids = []
        for obj in self.gate.store.list(
            prefix=self.gate.meta.parentset_id, suffix=self._glob
        ):
            ids.append(obj.uuid)
        self._batch_iter = iter(ids)

    def sampler(self):
        lst = list(self._batch_iter)
        self.__logger.info("File list %s", lst)
        try:
            rndidx = iter(self.random_state.choice(len(lst), self._nsamples))
        except ValueError:
            self.__logger.info("Cannot obtain random file list")
            raise

        for idx in rndidx:
            yield lst[idx]

    def __next__(self):
        return next(self._batch_iter)

    def generate(self):
        self.__logger.debug("Generating the file paths")
        # _files = pathlib.Path(self._path).glob(self._glob)
        _files = []
        glob_ = self.gate.store.list(prefix=self.gate.parentset_id, suffix=self._glob)
        for obj in glob_:
            _files.append(obj.uuid)
        for f in _files:
            self.__logger.debug(f)
        # return pathlib.Path(self._path).glob(self._glob)
        return iter(_files)
