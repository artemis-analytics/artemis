from setuptools import setup, find_packages

setup(name='artemis',
      version='0.0.1',
      author='Ryan White',
      author_email='ryan.white4@canada.ca',
      packages=find_packages(),
      install_requires=[
            "toposort>=1.5",
            "numpy",
            "pandas>=0.23.*",
            "scipy",
            "cython",
            "pyarrow>=0.11.*",
            "physt>=0.3.43",
            "histbook>=1.2.3",
            "packaging",
            "protobuff",
            "matplotlib"
            ],
      description="Stateful processing framework for administrative data powered by Apache Arrow")
