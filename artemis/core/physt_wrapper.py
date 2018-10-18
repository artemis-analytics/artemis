#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

from physt import h1

from .singleton import Singleton


class Physt_Wrapper(metaclass=Singleton):
    def __init__(self):
        self.hbook = {}

    def book(self, name):
        self.hbook[name] = []

    def fill(self, name, data, bins):
        self.hbook[name] = h1(data, bins)

    def get_histogram(self, name):
        return self.hbook[name]

    def to_pandas(self, name):
        return self.hbook[name].to_dataframe()

    def to_json(self, name):
        return self.hbook[name].to_json()
