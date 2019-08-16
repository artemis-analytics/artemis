#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Book of histograms
Derived from DIANA-HEP histbook package
Wraps Physt rather than histbook hist
BaseBook just implements common dictionary methods

ArtemisBook is concrete implementation for using
Physt histograms

Eventually, we'll need our own arrow-based histogram
"""

import collections
import fnmatch

import numpy as np

from physt.histogram_base import HistogramBase
from physt.histogram1d import Histogram1D
from physt.io.protobuf import write_many as physt_write_many
from physt.io.protobuf import read as physt_read
from physt.io.protobuf.histogram_pb2 import HistogramCollection

from artemis.logger import Logger
from artemis.utils.utils import autobinning

from tdigest import TDigest
from artemis.io.protobuf.tdigest_pb2 import TDigest_store, TDigest_instance


class BaseBook(collections.MutableMapping):

    def __init__(self, hists={}):

        self._content = collections.OrderedDict()

        if isinstance(hists, dict):
            for n, x in hists.items():
                self[n] = x

        self._updated()

    @classmethod
    def load_from_dicts(cls, content):
        out = cls.__new__(cls)
        out._content = collections.OrderedDict()

        for k, v in content.items():
            out[k] = v

        return out

    def compatible(self, other):
        '''
        books have equivalent keys
        re-implement in derived classes
        '''

        return set(self._iter_keys()) == set(other._iter_keys())

    def _updated(self):
        pass

    def __eq__(self, other):
        '''
        book1 == book2
        '''
        return self.__class__ == other.__class__ \
            and self._content == other._content

    def __ne__(self, other):
        '''
        book1 != book2
        '''
        return not self.__eq__(other)

    def __len__(self):
        '''
        len(book)
        '''
        return len(self._content)

    def __contains__(self, name):
        '''
        if book has key
        '''
        try:
            self[name]
        except KeyError:
            return False
        else:
            return True

    def _get(self, name):
        attempt = self._content.get(name, None)
        if attempt is not None:
            return attempt
        return None

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError("keys of a {0} must be strings".
                            format(self.__class__.__name__))

        if "*" in name:
            return [x for n, x in self if fnmatch.fnmatchcase(n, name)]
        else:
            out = self._get(name)
            if out is not None:
                return out
            else:
                raise KeyError("could not find {0} and could not interpret \
                                as a glob pattern".format(repr(name)))

    def _set(self, name, value):
        self._content[name] = value
        self._updated()

    def __setitem__(self, name, value):
        '''
        book[key] = value
        '''

        if not isinstance(name, str):
            raise TypeError
        if not isinstance(value, HistogramBase):
            raise TypeError

        self._set(name, value)

    def _del(self, name):
        if name in self._content:
            del self._content[name]
            self._updated()
        else:
            raise KeyError

    def __delitem__(self, name):
        '''
        del book[key]
        '''
        if not isinstance(name, str):
            raise TypeError

        if '*' in name:
            keys = [n for n in self._contents.keys()
                    if fnmatch.fnmatchcase(n, name)]
            for k in keys:
                self._del(k)
        else:
            self._del(name)

    def __iter__(self):
        '''
        for k, v in book.items()
        '''
        for k, v in self._content.items():
            yield k, v

    def _iter_keys(self):
        for k, v in self._content.items():
            yield k

    def _iter_values(self):
        for k, v in self._content.items():
            yield v

    def keys(self):
        return list(self._iter_keys())

    def values(self):
        return list(self._iter_values())

    def items(self):
        '''
        book.items()
        '''
        return list(self._content.items())

    def __add__(self, other):
        '''
        book = book1 + book2
        '''
        if not isinstance(other, BaseBook):
            raise TypeError("histogram books can only be added to other books")

        content = collections.OrderedDict()
        for n, x in self:
            if n in other:
                content[n] = x + other[n]
            else:
                content[n] = x
        for n, x in other:
            if n not in self:
                content[n] = x
        return self.__class__.load_from_dicts(content)

    def __iadd__(self, other):
        '''
        book += book1
        '''
        if not isinstance(other, BaseBook):
            raise TypeError("books can only be added to other books")

        for n, x in other:
            if n not in self:
                self[n] = x
            else:
                self[n] += x
        return self


@Logger.logged
class ArtemisBook(BaseBook):
    '''
    Concrete implementation for Physt histograms and timers
    '''
    def __init__(self, hists={}):
        super().__init__(hists)
        self._timers = collections.OrderedDict()
        self._rebooked = False

    def compatible(self, other):
        return set(self._iter_keys()) == set(other._iter_keys()) and \
                all(self[n].has_same_bins(other[n]) for n in self.keys())

    def reset(self):
        '''
        clear bin contents of all histograms
        '''
        pass

    def copy(self):
        '''
        return copy w or w/o bin contents
        '''
        pass

    def write(self):
        '''
        serialize and write to file
        '''
        pass

    def book(self, algname, name, bins, axis_name=None, timer=False):
        name_ = '.'
        name_ = name_.join([algname, name])
        self.__logger.info("Booking %s", name_)
        # TODO
        # Explore more options for correctly initializing h1
        value = self._get(name)
        if value is not None:
            self.__logger.error("Histogram already exists %s", name_)
        else:
            try:
                h = Histogram1D(bins, stats={"sum": 0.0, "sum2": 0.0})
            except Exception:
                self.__logger.error("Physt fails to book")
                raise
            self[name_] = h

            if timer is True:
                self._timers[name_] = []

        if axis_name:
            self._get(name_).axis_name = axis_name

    def rebook(self, excludes=[]):
        '''
        Force reset of all histograms with a copy
        do NOT include copying data
        '''

        self._rebooked = True
        for n, x in self:
            if n in excludes:
                continue
            timer = self._timers.get(n, None)
            if timer is None:
                bins = x.binning
            else:
                try:
                    bins = autobinning(timer)
                except IndexError:
                    self.__logger.warning("%s fails rebook, use original bins",
                                          n)
                    bins = x.binning

                del self._timers[n]
            self._set(n, Histogram1D(bins, stats={"sum": 0.0, "sum2": 0.0}))

    def _fill_timer(self, algname, name, data):
        name_ = algname + '.' + name
        if(isinstance(data, list)):
            self._timers[name_].extend(data)
        elif(isinstance(data, np.ndarray)):
            self._timers[name_].extend(list(data))
        else:
            self._timers[name_].append(data)

    def fill(self, algname, name, data):
        name_ = algname + '.' + name
        if(isinstance(data, list)):
            data = np.asarray(data)
            self._get(name_).fill_n(data)
        elif(isinstance(data, np.ndarray)):
            self._get(name_).fill_n(data)
        else:
            self._get(name_).fill(data)

        if self._rebooked is False:
            if name_ in self._timers.keys():
                self._fill_timer(algname, name, data)

    def _from_message(self, msg):

        content = collections.OrderedDict((n, physt_read(v))
                                          for n, v in msg.histograms.items())

        return self.__class__.load_from_dicts(content)

    def _to_message(self):
        return physt_write_many(self._content)

    def load(self, fname):
        msg = HistogramCollection()
        try:
            with open(fname, 'rb') as f:
                msg.ParseFromString(f.read())
        except IOError:
            print("Cannot read collections")
        except Exception:
            raise
        return self._from_message(msg)

    def finalize(self, fname):
        try:
            with open(fname, "wb") as f:
                f.write(self._to_message().SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
            self.__logger.error(fname)
        except Exception:
            raise


@Logger.logged
class TDigestBook(BaseBook):
    '''
    Concrete implementation for TDigest objects and serialization
    '''
    def __init__(self, tdigests={}):
        super().__init__(tdigests)
        self._rebooked = False

    def compatible(self, other):
        return set(self._iter_keys()) == set(other._iter_keys()) and \
                all(self[n].has_same_bins(other[n]) for n in self.keys())

    def __setitem__(self, name, value):
        '''
        book[key] = value
        '''

        if not isinstance(name, str):
            raise TypeError
        if not isinstance(value, TDigest):
            raise TypeError

        self._set(name, value)

    def reset(self):
        '''
        clear bin contents of all histograms
        '''
        pass

    def copy(self):
        '''
        return copy w or w/o bin contents
        '''
        pass

    def write(self):
        '''
        serialize and write to file
        '''
        pass

    def book(self, algname, name):
        name_ = '.'
        name_ = name_.join([algname, name])
        self.__logger.info("Booking %s", name_)
        # TODO
        # Explore more options for correctly initializing h1
        value = self._get(name)
        if value is not None:
            self.__logger.error("TDigest already exists %s", name_)
        else:
            try:
                h = TDigest()
            except Exception:
                self.__logger.error("TDigest fails to book")
                raise
            self[name_] = h

    def rebook(self, excludes=[]):
        '''
        Force reset of all tdigests
        '''

        self._rebooked = True
        for n, x in self:
            if n in excludes:
                continue
            self._set(n, TDigest())
        pass

    def fill(self, algname, name, data):
        # name_ = algname + '.' + name
        pass

    def _digest_to_protobuf(self, digest, name):

        '''
        Private function that converts a TDigest object
        to a google protocol buffer object

        Input: TDigest object, the name of the TDigest objects name
        (this is the name of the column in the Artemis project)
        Returns: google protocol buffer object TDigest_instance
        '''
        # Convert the input TDigest object into dictionary format
        digest_dict = digest.to_dict()

        # Declare the TDigest object that will be returned
        protobuf_instance = TDigest_instance()

        protobuf_instance.name = name
        protobuf_instance.K = digest_dict["K"]
        protobuf_instance.delta = digest_dict["delta"]
        protobuf_instance.n = digest_dict["n"]

        # Extract the centroids and weights from the digest map
        centroids_and_weights_map = digest_dict["centroids"]

        for i in range(len(centroids_and_weights_map)):
            # Delicate the Centroid_map object
            # which we will then populate with the values of the
            current_centroid = protobuf_instance.centroids.add()
            try:
                current_centroid.c = centroids_and_weights_map[i]["c"]
                current_centroid.m = centroids_and_weights_map[i]["m"]
            except Exception:
                self.__logger.error("Error: unable to add centroids")
                raise

        return protobuf_instance

    def _digest_from_protobuf(self, protobuf):

        # Ensure that the input protobuf is indeed a protobuf object
        if not isinstance(protobuf, TDigest_instance):
            raise TypeError("Error: tried to decode a "
                            "non protobuf object into a TDigest")

        digest_dict = {}

        digest_dict['K'] = protobuf.K
        digest_dict['delta'] = protobuf.delta

        centroid_list = []

        for centroid in protobuf.centroids:
            current_centroid = {}
            current_centroid['c'] = centroid.c
            current_centroid['m'] = centroid.m
            centroid_list.append(current_centroid)

        digest_dict['centroids'] = centroid_list

        digest = TDigest()
        digest.update_from_dict(digest_dict)
        return digest

    def _from_message(self, msg):

        content = collections.OrderedDict((n, self._digest_from_protobuf(v))
                                          for n, v in msg.digest_map.items())
        return self.__class__.load_from_dicts(content)

    def _to_message(self):
        store = TDigest_store()
        for n, x in self:
            store.digest_map[n].CopyFrom(self._digest_to_protobuf(x, n))
        return store

    @classmethod
    def load(cls, fname):
        msg = TDigest_store()
        out = cls.__new__(cls)
        try:
            with open(fname, 'rb') as f:
                msg.ParseFromString(f.read())
        except IOError:
            print("Cannot read collections")
        except Exception:
            raise
        try:
            return out._from_message(msg)
        except Exception:
            print("Fail to load from msg")
            raise

    def finalize(self, fname):
        try:
            with open(fname, "wb") as f:
                f.write(self._to_message().SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
            self.__logger.error(fname)
        except Exception:
            raise
