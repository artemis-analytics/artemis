import unittest
from faker import Faker
from faker.providers import BaseProvider
from numpy.random import binomial

class Provider(BaseProvider):

    def binom_dist(self, params):
        n, p = [float(x) for x in params]
        return binomial(n, p, size=1)

