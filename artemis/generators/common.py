#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Base class for collection of data generators

Refer to arrow/python/pyarrow/benchmarks/common.py

Common utils
Common Base Builtin Generator

"""

import sys

import itertools
import codecs
import decimal
import unicodedata
import importlib

from functools import partial
from pprint import pformat

import numpy as np
import numbers

import pyarrow as pa

from artemis.logger import Logger
from artemis.core.algo import AbcAlgoBase
from artemis.core.properties import JobProperties, Properties
from artemis.io.protobuf.artemis_pb2 import Algo as Algo_pb
from artemis.errors import AbstractMethodError

KILOBYTE = 1 << 10
MEGABYTE = KILOBYTE * KILOBYTE

DEFAULT_NONE_PROB = 0.0


def check_random_state(seed):
    '''
    Turn seed into a numpy.random.RandomState instance
    Ensures if using multiple generators in code we avoid
    repeatability problems

    https://scikit-learn.org/stable/developers/utilities.html#validation-tools

    Parameters
    ----------
    seed: None | int | instance of RandomState

    '''
    if seed is None or seed is np.random:
        return np.random.mtrand._rand  # returns numpy singleton RandomState
    if isinstance(seed, (numbers.Integral, np.integer)):
        return np.random.RandomState(seed)
    if isinstance(seed, np.random.RandomState):
        return seed
    raise ValueError('%r cannot be used to seed a \
        numpy.random.RandomState instance ' % seed)


def _multiplicate_sequence(base, target_size):
    q, r = divmod(target_size, len(base))
    return [base] * q + [base[:r]]


def get_random_bytes(n, seed=42):
    """
    Generate a random bytes object of size *n*.
    Note the result might be compressible.
    """
    rnd = np.random.RandomState(seed)
    # Computing a huge random bytestring can be costly, so we get at most
    # 100KB and duplicate the result as needed
    base_size = 100003
    q, r = divmod(n, base_size)
    if q == 0:
        result = rnd.bytes(r)
    else:
        base = rnd.bytes(base_size)
        result = b''.join(_multiplicate_sequence(base, n))
    assert len(result) == n
    return result


def get_random_ascii(n, seed=42):
    """
    Get a random ASCII-only unicode string of size *n*.
    """
    arr = np.frombuffer(get_random_bytes(n, seed=seed), dtype=np.int8) & 0x7f
    result, _ = codecs.ascii_decode(arr)
    assert isinstance(result, str)
    assert len(result) == n
    return result


def _random_unicode_letters(n, seed=42):
    """
    Generate a string of random unicode letters (slow).
    """
    def _get_more_candidates():
        return rnd.randint(0, sys.maxunicode, size=n).tolist()

    rnd = np.random.RandomState(seed)
    out = []
    candidates = []

    while len(out) < n:
        if not candidates:
            candidates = _get_more_candidates()
        ch = chr(candidates.pop())
        # XXX Do we actually care that the code points are valid?
        if unicodedata.category(ch)[0] == 'L':
            out.append(ch)
    return out


_1024_random_unicode_letters = _random_unicode_letters(1024)


def get_random_unicode(n, seed=42):
    """
    Get a random non-ASCII unicode string of size *n*.
    """
    indices = np.frombuffer(get_random_bytes(n * 2, seed=seed),
                            dtype=np.int16) & 1023
    unicode_arr = np.array(_1024_random_unicode_letters)[indices]

    result = ''.join(unicode_arr.tolist())
    assert len(result) == n, (len(result), len(unicode_arr))
    return result


class GeneratorBase(metaclass=AbcAlgoBase):
    '''
    Common base class for generators
    '''

    def __init__(self, name, **kwargs):
        '''
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc...
        can be done through the classmethods

        Can we use staticmethods in artemis to make uniform
        formatting of info, debug, warn, error?
        '''
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug('__init__ GeneratorBase')

        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

        self._jp = JobProperties()

        if hasattr(self.properties, 'seed'):
            self._builtin_generator = BuiltinsGenerator(self.properties.seed)
        else:
            self._builtin_generator = BuiltinsGenerator()

        if hasattr(self.properties, 'nbatches'):
            self._nbatches = self.properties.nbatches
            self._batch_iter = iter(range(self.properties.nbatches))
        else:
            self.__logger.warning("Number of batches not defined")

    @property
    def random_state(self):
        return self._builtin_generator.rnd

    @property
    def name(self):
        '''
        Algorithm name
        '''
        return self.__name

    def reset(self):
        if hasattr(self, '_nbatches'):
            self._batch_iter = iter(range(self._nbatches))
        else:
            self.__logger.warning("Override reset in concrete class")

    def to_msg(self):
        message = Algo_pb()
        message.name = self.name
        message.klass = self.__class__.__name__
        message.module = self.__module__
        message.properties.CopyFrom(self.properties.to_msg())
        return message

    @staticmethod
    def from_msg(logger, msg):
        logger.info('Loading Algo from msg %s', msg.name)
        try:
            module = importlib.import_module(msg.module)
        except ImportError:
            logger.error('Unable to load module %s', msg.module)
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, msg.klass)
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (msg.name, msg.klass))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        properties = Properties.from_msg(msg.properties)
        logger.debug(pformat(properties))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if 'loglevel' not in properties:
            properties['loglevel'] = \
                    logger.getEffectiveLevel()

        try:
            instance = class_(msg.name, **properties)
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (msg.name, 'properties'))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def generate(self):
        pass

    def initialize(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise AbstractMethodError(self)

    def sampler(self):
        raise AbstractMethodError(self)


class BuiltinsGenerator(object):

    def __init__(self, seed=42):
        self.rnd = check_random_state(seed)

    def sprinkle(self, lst, prob, value):
        """
        Sprinkle *value* entries in list *lst* with likelihood *prob*.
        """
        for i, p in enumerate(self.rnd.random_sample(size=len(lst))):
            if p < prob:
                lst[i] = value

    def sprinkle_nones(self, lst, prob):
        """
        Sprinkle None entries in list *lst* with likelihood *prob*.
        """
        self.sprinkle(lst, prob, None)

    def generate_int_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python ints with *none_prob* probability of
        an entry being None.
        """
        data = list(range(n))
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_float_list(self, n, none_prob=DEFAULT_NONE_PROB,
                            use_nan=False):
        """
        Generate a list of Python floats with *none_prob* probability of
        an entry being None (or NaN if *use_nan* is true).
        """
        # Make sure we get Python floats, not np.float64
        data = list(map(float, self.rnd.uniform(0.0, 1.0, n)))
        assert len(data) == n
        self.sprinkle(data, none_prob, value=float('nan') if use_nan else None)
        return data

    def generate_bool_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python bools with *none_prob* probability of
        an entry being None.
        """
        # Make sure we get Python bools, not np.bool_
        data = [bool(x >= 0.5) for x in self.rnd.uniform(0.0, 1.0, n)]
        assert len(data) == n
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_decimal_list(self, n, none_prob=DEFAULT_NONE_PROB,
                              use_nan=False):
        """
        Generate a list of Python Decimals with *none_prob* probability of
        an entry being None (or NaN if *use_nan* is true).
        """
        data = [decimal.Decimal('%.9f' % f)
                for f in self.rnd.uniform(0.0, 1.0, n)]
        assert len(data) == n
        self.sprinkle(data, none_prob,
                      value=decimal.Decimal('nan') if use_nan else None)
        return data

    def generate_object_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of generic Python objects with *none_prob*
        probability of an entry being None.
        """
        data = [object() for i in range(n)]
        self.sprinkle_nones(data, none_prob)
        return data

    def _generate_varying_sequences(self, random_factory, n, min_size,
                                    max_size, none_prob):
        """
        Generate a list of *n* sequences of varying size between *min_size*
        and *max_size*, with *none_prob* probability of an entry being None.
        The base material for each sequence is obtained by calling
        `random_factory(<some size>)`
        """
        base_size = 10000
        base = random_factory(base_size + max_size)
        data = []
        for i in range(n):
            off = self.rnd.randint(base_size)
            if min_size == max_size:
                size = min_size
            else:
                size = self.rnd.randint(min_size, max_size + 1)
            data.append(base[off:off + size])
        self.sprinkle_nones(data, none_prob)
        assert len(data) == n
        return data

    def generate_fixed_binary_list(self, n, size, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a fixed *size*.
        """
        return self._generate_varying_sequences(get_random_bytes, n,
                                                size, size, none_prob)

    def generate_varying_binary_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_bytes, n,
                                                min_size, max_size, none_prob)

    def generate_ascii_string_list(self, n, min_size, max_size,
                                   none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of ASCII strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_ascii, n,
                                                min_size, max_size, none_prob)

    def generate_unicode_string_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of unicode strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_unicode, n,
                                                min_size, max_size, none_prob)

    def generate_int_list_list(self, n, min_size, max_size,
                               none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of lists of Python ints with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(
            partial(self.generate_int_list, none_prob=none_prob),
            n, min_size, max_size, none_prob)

    def generate_tuple_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of tuples with random values.
        Each tuple has the form `(int value, float value, bool value)`
        """
        dicts = self.generate_dict_list(n, none_prob=none_prob)
        tuples = [(d.get('u'), d.get('v'), d.get('w'))
                  if d is not None else None
                  for d in dicts]
        assert len(tuples) == n
        return tuples

    def generate_dict_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of dicts with random values.
        Each dict has the form

            `{'u': int value, 'v': float value, 'w': bool value}`
        """
        ints = self.generate_int_list(n, none_prob=none_prob)
        floats = self.generate_float_list(n, none_prob=none_prob)
        bools = self.generate_bool_list(n, none_prob=none_prob)
        dicts = []
        # Keep half the Nones, omit the other half
        keep_nones = itertools.cycle([True, False])
        for u, v, w in zip(ints, floats, bools):
            d = {}
            if u is not None or next(keep_nones):
                d['u'] = u
            if v is not None or next(keep_nones):
                d['v'] = v
            if w is not None or next(keep_nones):
                d['w'] = w
            dicts.append(d)
        self.sprinkle_nones(dicts, none_prob)
        assert len(dicts) == n
        return dicts

    def get_type_and_builtins(self, n, type_name):
        """
        Return a `(arrow type, list)` tuple where the arrow type
        corresponds to the given logical *type_name*, and the list
        is a list of *n* random-generated Python objects compatible
        with the arrow type.
        """
        size = None

        if type_name in ('bool', 'decimal', 'ascii', 'unicode', 'int64 list'):
            kind = type_name
        elif type_name.startswith(('int', 'uint')):
            kind = 'int'
        elif type_name.startswith('float'):
            kind = 'float'
        elif type_name.startswith('struct'):
            kind = 'struct'
        elif type_name == 'binary':
            kind = 'varying binary'
        elif type_name.startswith('binary'):
            kind = 'fixed binary'
            size = int(type_name[6:])
            assert size > 0
        else:
            raise ValueError("unrecognized type %r" % (type_name,))

        if kind in ('int', 'float'):
            ty = getattr(pa, type_name)()
        elif kind == 'bool':
            ty = pa.bool_()
        elif kind == 'decimal':
            ty = pa.decimal128(9, 9)
        elif kind == 'fixed binary':
            ty = pa.binary(size)
        elif kind == 'varying binary':
            ty = pa.binary()
        elif kind in ('ascii', 'unicode'):
            ty = pa.string()
        elif kind == 'int64 list':
            ty = pa.list_(pa.int64())
        elif kind == 'struct':
            ty = pa.struct([pa.field('u', pa.int64()),
                            pa.field('v', pa.float64()),
                            pa.field('w', pa.bool_())])

        factories = {
            'int': self.generate_int_list,
            'float': self.generate_float_list,
            'bool': self.generate_bool_list,
            'decimal': self.generate_decimal_list,
            'fixed binary': partial(self.generate_fixed_binary_list,
                                    size=size),
            'varying binary': partial(self.generate_varying_binary_list,
                                      min_size=3, max_size=40),
            'ascii': partial(self.generate_ascii_string_list,
                             min_size=3, max_size=40),
            'unicode': partial(self.generate_unicode_string_list,
                               min_size=3, max_size=40),
            'int64 list': partial(self.generate_int_list_list,
                                  min_size=0, max_size=20),
            'struct': self.generate_dict_list,
            'struct from tuples': self.generate_tuple_list,
        }
        data = factories[kind](n)
        return ty, data
