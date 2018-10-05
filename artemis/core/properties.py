#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Property classes
"""

from collections import OrderedDict

from artemis.core.singleton import Singleton


class Properties():
    '''
    Dynamically create getter/setter for user-defined properties
    '''
    def __init__(self, lock=False):
        self.lock = lock
        self.properties = dict()

    def add_property(self, name, value):
        # Retain dictionary of properties
        self.properties[name] = value
        # Local fget and fset functions
        # lambdas defined directly in property below
        # fixes flake8 errors

        # add the property to self
        setattr(self.__class__, name,
                property(lambda self: self._get_property(name),
                         lambda self, value: self._set_property(name, value)))
        # add corresponding local variable
        setattr(self, '_' + name, value)

    def to_dict(self):
        '''
        Ordered dictionary of all user-defined properties
        '''
        _dict = OrderedDict()
        for key in self.properties:
            _dict[key] = self.properties[key]
        return _dict

    def _set_property(self, name, value):
        if not self.lock:
            setattr(self, '_' + name, value)
        else:
            print('Cannot change "{}": properties are locked'
                  .format(name))

    def _get_property(self, name):
        return getattr(self, '_' + name)


class JobProperties(metaclass=Singleton):
    '''
    Wrapper class as Singleton type
    Holds JobProperties for use throughout framework
    '''
    def __init__(self):
        self.data = OrderedDict()
