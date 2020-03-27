#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Collection of utility functions
"""
import statistics


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


def autobinning(lst, nbins=10):
    try:
        mu = statistics.mean(lst)
        std = statistics.stdev(lst)
    except statistics.StatisticsError:
        mu = lst[-1]
        std = mu
    except Exception:
        raise

    lower_edge = mu - 5 * std
    if lower_edge < 0.0:
        lower_edge = 0.0

    upper_edge = mu + 5 * std
    range_ = upper_edge - lower_edge
    digs, order = ("%0.20e" % range_).split("e")
    order = abs(int(order))

    lower_edge = round(lower_edge, order)
    upper_edge = round(upper_edge, order)
    bin_width = round(range_ / nbins, order + 1)
    bins = [x for x in range_positive(lower_edge, upper_edge, bin_width)]
    return bins
