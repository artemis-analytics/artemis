#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

import numpy as np

from physt.histogram1d import Histogram1D
from physt.io.protobuf import write_many

from .singleton import Singleton

from artemis.logger import Logger


@Logger.logged
class Physt_Wrapper(metaclass=Singleton):
    def __init__(self):
        self.hbook = {}

    def book(self, algname, name, bins, axis_name=None):
        name_ = '.'
        name_ = name_.join([algname, name])
        # TODO
        # Explore more options for correctly initializing h1
        value = self.hbook.get(name_, None)
        if value is not None:
            self.__logger.error("Histogram already exists")
        else:
            try:
                # self.__logger.debug(bins)
                self.hbook[name_] = \
                        Histogram1D(bins,
                                    stats={"sum": 0.0, "sum2": 0.0})
            except Exception:
                self.__logger.error("Physt fails to book")

        if axis_name:
            self.hbook[name_].axis_name = axis_name

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
            except Exception:
                self.__logger.error("Physt fails to book")

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
