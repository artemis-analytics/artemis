#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Yet another data sink
for the timers
"""

from .singleton import Singleton
import statistics


class TimerSvc(metaclass=Singleton):
    def __init__(self):
        self.timer_dict = {}

    @property
    def keys(self):
        return self.timer_dict.keys()

    def name(self, algname, key):
        _name = '.'
        _name = _name.join([algname, 'time', key])
        return _name

    def fill(self, algname, key, time):
        self.timer_dict[self.name(algname, key)].append(time)

    def get_timer(self, algname, key):
        return self.timer_dict[self.name(algname, key)]

    def book(self, algname, key):
        self.timer_dict[self.name(algname, key)] = []

    def contains(self, key):
        return key in self.timer_dict

    def stats(self, algname, key):
        _name = self.name(algname, key)
        try:
            mu = statistics.mean(self.timer_dict[_name])
            std = statistics.stdev(self.timer_dict[_name])
        except statistics.StatisticsError:
            mu = self.timer_dict[_name][-1]
            std = mu
        except Exception:
            raise
        return mu, std
