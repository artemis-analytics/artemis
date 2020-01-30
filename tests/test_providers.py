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

import unittest
import tempfile
from faker import Faker
from artemis.generators.simutable.providers.chi import Provider as ChiSquare
from artemis.generators.simutable.providers.binom import Provider as Binomial
from artemis.generators.simutable.providers.gamma import Provider as Gamma
from artemis.generators.simutable.providers.norm import Provider as Normal
from artemis.generators.simutable.providers.lognormal import Provider as LogNormal
from artemis.generators.simutable.providers.bernoulli import Provider as Bernoulli
from artemis.generators.simutable.providers.poisson import Provider as Poisson

class DistCase(unittest.TestCase):

    def test_chisq(self):
        fake = Faker()
        provider = ChiSquare(fake)
        fake.add_provider(provider)
        print(fake.chi_square_dist(1))

    def test_binom(self):
        fake = Faker()
        provider = Binomial(fake)
        fake.add_provider(provider)
        print(fake.binom_dist(["5", "0.4"]))

    def test_bernoulli(self):
        fake = Faker()
        provider = Bernoulli(fake)
        fake.add_provider(provider)
        print(fake.bernoulli_dist(1))

    def test_gamma(self):
        fake = Faker()
        provider = Gamma(fake)
        fake.add_provider(provider)
        print(fake.gamma_dist(0.9))

    def test_lognormal(self):
        fake = Faker()

        provider = LogNormal(fake)
        fake.add_provider(provider)
        print(fake.lognormal())

    def test_poisson(self):
        fake = Faker()
        provider = Poisson(fake)
        fake.add_provider(provider)
        print(fake.poisson_dist(1))

    def test_normal(self):
        fake = Faker()
        provider = Normal(fake)
        fake.add_provider(provider)
        print(fake.normal_dist(["1", "2"]))

if __name__ == '__main__':
    print('Unit Test: Faker')
    unittest.main()
