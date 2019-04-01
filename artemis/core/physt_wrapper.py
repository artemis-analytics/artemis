#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

import numpy as np
import os

from physt.histogram1d import Histogram1D
from physt.io.protobuf import write_many

from .singleton import Singleton

from artemis.logger import Logger


@Logger.logged
class Physt_Wrapper(metaclass=Singleton):
    def __init__(self):
        self.hbook = {}
        self._job_id = None
        self._path = None

    @property
    def job_id(self):
        return self._job_id

    @job_id.setter
    def job_id(self, value):
        self._job_id = value

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value

    def book(self, algname, name, bins, axis_name=None):
        name_ = '.'
        name_ = name_.join([algname, name])
        # TODO
        # Explore more options for correctly initializing h1
        value = self.hbook.get(name_, None)
        if value is not None:
            self.__logger.error("Histogram already exists %s", name_)
        else:
            try:
                # self.__logger.debug(bins)
                self.hbook[name_] = \
                        Histogram1D(bins,
                                    stats={"sum": 0.0, "sum2": 0.0})
            except Exception:
                self.__logger.error("Physt fails to book")
                raise

        if axis_name:
            self.hbook[name_].axis_name = axis_name

    def rebook_all(self, excludes=[]):
        '''
        Force reset of all histograms with a copy
        do NOT include copying data
        '''
        for key in self.hbook.keys():
            if key in excludes:
                continue
            try:
                bins = self.hbook[key].binning
            except KeyError:
                self.__logger.error("%s not found", key)
            except Exception:
                self.__logger.error("%s failed to retrieve bins", key)
            self.hbook[key] = Histogram1D(bins,
                                          stats={"sum": 0.0, "sum2": 0.0})
            self.logger.debug('Rebook %s %s', key, self.hbook[key])

    def rebook(self, algname, name, bins, axis_name=None):
        name_ = '.'
        name_ = name_.join([algname, name])
        # TODO
        # Explore more options for correctly initializing h1
        value = self.hbook.get(name_, None)
        if value is None:
            self.__logger.error("Histogram does not exist, use book() instead")
        else:
            try:
                # self.__logger.debug(bins)
                self.hbook[name_] = \
                        Histogram1D(bins,
                                    stats={"sum": 0.0, "sum2": 0.0})
            except KeyError:
                self.__logger.error("Histogram not found %s", name_)
                raise
            except Exception:
                self.__logger.error("Unknown error")
                raise

        if axis_name:
            self.hbook[name_].axis_name = axis_name

    def fill(self, algname, name, data):
        name_ = algname + '.' + name
        if(isinstance(data, list)):
            data = np.asarray(data)
            self.hbook[name_].fill_n(data)
        elif(isinstance(data, np.ndarray)):
            self.hbook[name_].fill_n(data)
        else:
            self.hbook[name_].fill(data)

    def get_histogram(self, algname, name):
        name_ = algname + '.' + name
        try:
            return self.hbook[name_]
        except KeyError:
            raise

    def to_pandas(self, algname, name):
        name_ = algname + '.' + name
        return self.hbook[name_].to_dataframe()

    def to_json(self, algname, name):
        name_ = algname + '.' + name
        return self.hbook[name_].to_json()

    def to_message(self):
        return write_many(self.hbook)

    def finalize(self):
        hcname = self._job_id + '.hist.dat'
        hcname = os.path.join(self._path, hcname)
        try:
            with open(hcname, "wb") as f:
                f.write(self.to_message().SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
            self.__logger.error(hcname)
        except Exception:
            raise
