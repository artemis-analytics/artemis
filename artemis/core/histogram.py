#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

from histbook import Book, Hist
from histbook.axis import bin
from .singleton import Singleton


class Histogram(metaclass=Singleton):
    def __init__(self):
        self.hbook = Book()

    def book(self, name, numbins, minimum, maximum):
        self.hbook[name] = Hist(bin(name, numbins, minimum, maximum))

    def fill(self, name, array):
        kwargs = {}
        kwargs[name] = array
        self.hbook[name].fill(**kwargs)

    def get_histogram(self, name):
        return self.hbook[name]

    def to_pandas(self, name):
        return self.get_histogram(name).pandas()

    def to_json(self, name):
        return self.get_histogram(name).pandas().to_json(orient='records')
