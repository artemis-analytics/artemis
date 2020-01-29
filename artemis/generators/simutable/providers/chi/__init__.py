from faker import Faker
from faker.providers import BaseProvider
from scipy.stats import chi2

class Provider(BaseProvider):

    def chi_square_dist(self, params):
        return chi2.rvs(params, size=1)
        
