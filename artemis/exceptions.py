#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Custom exceptions for Artemis
"""


class NullDataError(Exception):
    pass


class MissingDataError(Exception):
    pass
