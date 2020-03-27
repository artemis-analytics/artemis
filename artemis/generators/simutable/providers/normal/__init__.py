# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#

"""
faker provider for creating a normal distribution
"""

import unittest

from faker import Faker
from faker.providers import BaseProvider


class Provider(BaseProvider):
    def normal(self):
        mu = 0
        sigma = 1

        return self.generator.random.normalvariate(mu, sigma)


class TestCase(unittest.TestCase):
    def test(self):
        fake = Faker()

        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.normal())


if __name__ == "__main__":
    unittest.main()
