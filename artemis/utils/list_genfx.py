#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

'''
List of generator functions from faker and local providers
'''
from artemis.generators.simutable.loader import PROVIDERS
from faker import Faker

# Return a list of generator functions from simutable/provider
def local_providers():
    provider_names = []
    for provider in PROVIDERS:
        provider_names.extend([x for x in 
            dir(provider.Provider) if not x.startswith('_')])
    return (list(dict.fromkeys(provider_names)))

# Return a list of generator functions from Faker
def faker_providers():
    faker = Faker('en_CA')
    provider_names = []
    for provider in faker.providers:
        provider_names.extend([x for x in 
            dir(provider) if not x.startswith('_')])
    return (list(dict.fromkeys(provider_names)))