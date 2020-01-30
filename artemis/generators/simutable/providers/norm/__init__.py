import unittest
from faker import Faker
from faker.providers import BaseProvider

class Provider(BaseProvider):

    def normal_dist(self, params):
        mu, sigma = [float(x) for x in params]
        return self.generator.random.normalvariate(mu, sigma)

