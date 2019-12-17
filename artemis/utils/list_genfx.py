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

'''
List of generator functions from faker and local providers
'''
from artemis.generators.simutable.loader import PROVIDERS
from faker import Faker


# Return a list of generator functions from simutable/provider
def local_providers():
    provider_names = []
    for provider in PROVIDERS:
        provider_names.extend([x for x in dir(provider.Provider)
                               if not x.startswith('_')])
    return (list(dict.fromkeys(provider_names)))


# Return a list of generator functions from Faker
def faker_providers():
    faker = Faker('en_CA')
    provider_names = []
    for provider in faker.providers:
        provider_names.extend([x for x in dir(provider)
                               if not x.startswith('_')])
    return (list(dict.fromkeys(provider_names)))
