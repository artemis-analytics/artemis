import unittest
from faker import Faker
from faker.providers import BaseProvider
from scipy.stats import bernoulli

class Provider(BaseProvider):

    def bernoulli_dist(self, params):
        return bernoulli.rvs(params, size=1)

class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.bernoulli_dist(1))