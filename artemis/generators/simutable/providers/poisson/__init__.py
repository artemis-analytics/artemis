import unittest
from faker import Faker
from faker.providers import BaseProvider
from scipy.stats import poisson

class Provider(BaseProvider):

    def gamma_dist(self, params):
        shape, scale = [float(x) for x in params]
        return gamma(shape=shape, scale=scale, size=1)

class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.gamma_dist(["1", "2"]))