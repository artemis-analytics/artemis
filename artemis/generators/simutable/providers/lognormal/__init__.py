# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#

"""
faker provider for creating a lognormal distribution
"""

import unittest

from faker import Faker
from faker.providers import BaseProvider


class Provider(BaseProvider):
    def lognormal(self):
        mu = 0
        sigma = 1

        return self.generator.random.lognormvariate(mu, sigma)


