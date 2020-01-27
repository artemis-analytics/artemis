import unittest
from faker import Faker
from faker.providers import BaseProvider
from numpy.random import binom

class Provider(BaseProvider):

    def binom_dist(self, params):
        n, p = [float(x) for x in params]
        return binom(n, p, size=1)

class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.binom_dist(["1", "2"]))