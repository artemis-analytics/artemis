#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
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
