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
    def normal(self, params):
        mu, sigma = check_double(params, normal_dist.__name__)
        return self.generator.random.normalvariate(mu, sigma)

class TemporaryErrorClass():
    def check_double(self, params, name):
        try:
            # Verify that the param is, in fact, a list
            assert isinstance(params, list)
        except AssertionError:
            raise TypeError("Illegal parameter type in %s", name)
        try:
            # Verify that the length is only 2
            assert len(params) == 2
        except AssertionError:
            # Complain if it is anything else
            if len(params) > 2:
                raise ValueError("Illegal parameters passed in function %s", name)
            else:
                raise ValueError("Insufficient parameters passed in function %s", name)
        try:
            # Verify you can unpack into X and Y as floats
            x, y = [float(x) for x in params]
            # Return those values
            return [x, y]
        # Complain if the values can't be formatted into floats
        except TypeError:
            raise TypeError("Illegal types in parameter array in function %s", name)

class TestCase(unittest.TestCase):
    def test(self, params):
        fake = Faker()

        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.normal())


if __name__ == '__main__':
    unittest.main()
