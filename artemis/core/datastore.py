#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

"""
Arrow PoC code.
"""

from .singleton import Singleton


class ArrowSets(metaclass=Singleton):
    def __init__(self):
        self.arrow_dict = {}

    def add_to_dict(self, key, batch):
        self.arrow_dict[key] = batch

    def get_data(self, key):
        return self.arrow_dict[key]

    def book(self, key):
        self.arrow_dict[key] = None

    def contains(self, key):
        return key in self.arrow_dict

    def is_empty(self):
        if not bool(self.arrow_dict):
            return True
        return False
