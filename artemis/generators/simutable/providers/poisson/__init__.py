import unittest
from faker import Faker
from faker.providers import BaseProvider
from scipy.stats import poisson

class Provider(BaseProvider):

    def poisson_dist(self, params):
        mu = float(params) 
        return poisson.rvs(mu, size=1)

