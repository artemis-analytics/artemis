# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#

"""
Following taken from faker/utils/loading
"""
import os
import sys
from importlib import import_module
import pkgutil


def get_path(module):
    if getattr(sys, 'frozen', False):
        path = os.path.dirname(sys.executable)
    else:
        path = os.path.dirname(os.path.realpath(module.__file__))
    return path

def list_module(module):
    path = get_path(module)
    modules = [name for finder, name,
               is_pkg in pkgutil.iter_modules([path]) if is_pkg]
    return modules

def find_available_providers(modules):
    available_providers = set()
    for providers_mod in modules:
        providers = [
                     '.'.join([providers_mod.__package__, mod])
                     for mod in list_module(providers_mod) if mod != '__pycache__'
                    ]
    available_providers.update(providers)
    return sorted(available_providers)
