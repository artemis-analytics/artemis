import unittest
from faker import Faker
from faker.providers import BaseProvider
from scipy.stats import gamma

class Provider(BaseProvider):
    
    def gamma_dist(self, params):
        shape=float(params)
        return gamma.rvs(shape, size=1)

