#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Various decorator methods
"""
import logging

def logged(obj):
    '''
    Taken from autologging.py
    Create a decorator to add logging to a class
    '''
    
    # Default use module name for logger
    # If AlgoBase use mro to set name
    logger_name = obj.__module__
    logger_attribute_name = '_' + obj.__name__ + '__logger'
    setattr(obj, logger_attribute_name, logging.getLogger(logger_name))
    
    return obj
