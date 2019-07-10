from setuptools import setup, find_packages

setup(name='artemis',
      version='0.4.0',
      author='Ryan White',
      author_email='ryan.white4@canada.ca',
      packages=find_packages(),
      install_requires=[
            "toposort>=1.5",
            "numpy",
            "pandas>=0.23.*",
            "scipy",
            "cython",
            "pyarrow>=0.12.*",
            "physt>=0.3.43",
            "histbook>=1.2.3",
            "packaging",
            "protobuf",
            "matplotlib",
            "sas7bdat>=2.2.2",
            "ebcdic",
            "faker",
            "tdigest"
            ],
      description="Stateful processing framework for administrative data powered by Apache Arrow")
