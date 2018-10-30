FROM registry.k8s.cloud.statcan.ca/stcdatascience/artemis/centos7-py36-arrow:v1

MAINTAINER Dominic Parent <dominic.parent@canada.ca>

RUN pip install --upgrade pip
RUN pip install setuptools cython packaging protobuf matplotlib

ADD artemis ./artemis
ADD tests ./tests
ADD setup.py ./setup.py

RUN python setup.py install
RUN python -m unittest
