import unittest
from faker import Faker
from faker.providers import BaseProvider
from numpy.random import gamma

class Provider(BaseProvider):
    
    def gamma_dist(self, params):
        shape=float(params)
        return gamma(shape=shape, scale=scale, size=1)

class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.bernoulli_dist(["1", "2"]))