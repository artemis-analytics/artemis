#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Singleton class to allow
creation of singleton objects, where appropriate.
Think of Singleton types as data sinks
"""


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                    super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def reset(cls):
        if cls in cls._instances:
            del cls._instances[cls]
        else:
            print('Key: ' + str(cls) + ' is not present.')

    def exists(cls):
        return cls in cls._instances
