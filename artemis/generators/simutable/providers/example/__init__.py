# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#

"""

"""
import unittest

from faker import Faker
from faker.providers import BaseProvider


class Provider(BaseProvider):
    def foo(self):
        return self.generator.random.randint(0, 100)


class TestCase(unittest.TestCase):
    def test(self):
        fake = Faker()

        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.foo())


if __name__ == "__main__":
    unittest.main()
