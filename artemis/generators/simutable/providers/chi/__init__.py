from faker import Faker
from faker.providers import BaseProvider
from numpy.random import chi2

class Provider(BaseProvider):

    def chi_square_dist(self, params):
        return chi2.rvs(params, size=1)
        
class TestCase(unittest.TestCase):

    def test(self):
        fake = Faker()
        provider = Provider(fake)
        fake.add_provider(provider)
        print(fake.chi_square_dist(1))