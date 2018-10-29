#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Collection of utility functions
"""


def bytes_to_mb(B):
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2)
    B /= MB
    precision = 1
    number_of_mb = round(B, precision)

    return number_of_mb


def range_positive(start, stop=None, step=None):
    if stop is None:
        stop = start + 0.0
        start = 0.0
    if step is None:
        step = 1.0
    while start < stop:
        yield start
        start += step
