#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
from importlib import import_module
from artemis.utils.loading import find_available_providers

META_PROVIDERS_MODULES = [
                          'artemis.generators.simutable.providers',
                          ]

PROVIDER_MODULES = \
        find_available_providers([import_module(path)
                                 for path in META_PROVIDERS_MODULES])

PROVIDERS = [import_module(module)
             for module in PROVIDER_MODULES]
