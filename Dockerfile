FROM python:3.7

MAINTAINER Dominic Parent <dominic.parent@canada.ca>

RUN pip install --upgrade pip
RUN pip install setuptools numpy pandas scipy cython pyarrow packaging protobuf matplotlib

ADD artemis ./artemis
ADD setup.py ./setup.py

RUN python setup.py install
