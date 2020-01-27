import unittest
from faker import Faker
from faker.providers import BaseProvider

class Provider(BaseProvider):

    def normal_dist(self, params):
        mu, sigma = [float(x) for x in params]
        return self.generator.random.normalvariate(mu, sigma)

class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.normal_dist(["1", "2"]))