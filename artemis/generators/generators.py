# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Module for test data generation
Largely taken from arrow/python/pyarrow/benchmarks/common.py
"""

import csv
import io
import sys
import string
import itertools
import codecs
import decimal
import unicodedata

import random
from array import array
from functools import partial

import numpy as np

import pyarrow as pa

from artemis.logger import Logger
from artemis.core.algo import AlgoBase

KILOBYTE = 1 << 10
MEGABYTE = KILOBYTE * KILOBYTE

DEFAULT_NONE_PROB = 0.0


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


class BuiltinsGenerator(object):

    def __init__(self, seed=42):
        self.rnd = np.random.RandomState(seed)

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


@Logger.logged
class GenCsvLike:
    '''
    Creates data in CSV format and sends bytes.
    Generator tries to guess at payload size
    for total number of floats to generate / column / chunk
    '''
    def __init__(self):
        '''
        Chunk configuration
        Data of ncolumns, of size <size> in <unit>.

        Number of chunks per requent
        nchunks

        Maximum number of requests
        maxchunks

        Up to client to be
        Ready for data
        '''
        self.ncolumns = 10
        self.units = 'm'
        self.size = 10
        self.nchunks = 10
        self.maxrequests = 1  # Equivalent to EOF?
        self._cntr = 0

    def gen_chunk(self, ncolumn, unit, size):
        '''
        Create a chunk of data of ncolumns, of size <size> in <unit>.
        '''
        units = {
                'b': 1,
                'k': 1000,
                'm': 1000000,
                'g': 1000000000,
                'B': 1,
                'K': 1000,
                'M': 1000000,
                'G': 1000000000}

        # Based off tests of random floats from random.random.
        float_size = 20

        # Total number of floats needed according to supplied criteria.
        nfloats = int((size * units[unit] / float_size))
        self.__logger.info("Total number of floats %s" % nfloats)

        # Total number of rows based off number of floats and required columns
        # nrows = int(nfloats / ncolumn)
        #
        chunk = ''
        floats = array('d', (random.random() for i in range(nfloats)))
        csv_rows = []
        csv_row = []
        i = 0
        j = 0
        # Initialize all variables above to avoid null references.
        columns = [[] for col in range(ncolumn)]

        while i < nfloats:
            # Generates list of rows.
            csv_row = []
            j = 0
            while j < ncolumn and i < nfloats:
                # Generates columns in each row.
                csv_row.append(floats[i])
                columns[j].append(floats[i])
                j += 1
                i += 1
            csv_rows.append(csv_row)

        # Use StringIO as an in memory file equivalent
        # (instead of with...open construction).
        output = io.StringIO()
        sio_f_csv = csv.writer(output)
        sio_f_csv.writerows(csv_rows)

        # Encodes the csv file as bytes.
        chunk = bytes(output.getvalue(), encoding='utf_8')
        return chunk

    def generate(self):
        self.__logger.info('Generate')
        self.__logger.info("%s: Producing Data" % (self.__class__.__name__))
        self.__logger.debug("%s: Producing Data" % (self.__class__.__name__))
        i = 0
        mysum = 0
        mysumsize = 0
        while i < self.nchunks:
            getdata = self.gen_chunk(20, 'm', 10)
            # Should be bytes.
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(getdata)))
            mysumsize += sys.getsizeof(getdata)
            mysum += len(getdata)
            i += 1
            yield getdata

        # Helped to figure out the math for an average float size.
        self.__logger.debug('%s: Average of total: %2.1f' %
                            (self.__class__.__name__, mysum/i))
        # Same as previous.
        self.__logger.debug('%s: Average of size: %2.1f' %
                            (self.__class__.__name__, mysumsize/i))


# @Logger.logged
class GenCsvLikeArrow(AlgoBase):
    '''
    Arrow-like generator
    see arrow/python/pyarrow/tests/test_csv.py

    tests specific number of rows and columns
    sends a batch rather than Table
    '''
    pa_types = ('int32', 'uint32', 'int64', 'uint64',
                'float32', 'float64')
    # TODO
    # bool type currently failing in pa.csv.read_csv
    # 'bool', 'decimal',
    # 'binary', 'binary10', 'ascii', 'unicode',
    # 'int64 list', 'struct', 'struct from tuples')

    def __init__(self, name, **kwargs):

        self._defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            self._defaults[key] = kwargs[key]

        # Set the properties with the full configuration
        super().__init__(name, **self._defaults)
        print(self.properties.to_dict())
        self.nbatches = self.properties.nbatches
        self.num_cols = self.properties.num_cols
        self.num_rows = self.properties.num_rows
        self.linesep = self.properties.linesep
        self.seed = self.properties.seed
        self.header = self.properties.header

        self._builtin_generator = BuiltinsGenerator(self.seed)
        self.types = []
        for _ in range(self.num_cols):
            self.types.append(self.pa_types
                              [random.randint(0, len(self.pa_types)-1)])
        print("WTFWTFWTFWTFWTF!!!!!!!!!!!!!")
        self.__logger.info("Initialized %s" % self.__class__.__name__)
        self.__logger.info(self.__class__.__dict__)

    def _set_defaults(self):
        defaults = {'nbatches': 1,
                    'num_cols': 2,
                    'num_rows': 10,
                    'linesep': u'\r\n',
                    'seed': 42,
                    'header': True}
        return defaults

    def generate_col_names(self):
        letters = string.ascii_lowercase
        # for letter in letters:
        #     yield letter

        for first in letters:
            for second in letters:
                yield first + second

    def make_random_csv(self):
        # Numpy generates column wise
        # Above we generate row-wise
        # Transpose to rows for csv
        arr = np.random.RandomState(self.seed).\
                randint(0, 1000, size=(self.num_cols, self.num_rows))

        col_names = list(itertools.islice(self.generate_col_names(),
                                          self.num_cols))

        # Simulates the write of csv file
        # Encode to bytes for processing, as above
        csv = io.StringIO()
        if self.header is True:
            csv.write(u",".join(col_names))
            csv.write(self.linesep)
        for row in arr.T:
            csv.write(u",".join(map(str, row)))
            csv.write(self.linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        columns = [pa.array(a, type=pa.int64()) for a in arr]
        expected = pa.RecordBatch.from_arrays(columns, col_names)
        return csv, col_names, expected

    def make_mixed_random_csv(self):
        '''
        Use arrow commons builtins to generate
        arrow arrays and push to csv
        precursor to using adds
        '''
        size = self.num_rows

        columns = [[] for _ in range(self.num_cols)]

        col_names = list(itertools.islice(self.generate_col_names(),
                                          self.num_cols))
        for icol in range(self.num_cols):
            ty, data = self._builtin_generator.\
                get_type_and_builtins(self.num_rows, self.types[icol])
            columns[icol] = data

        csv = io.StringIO()
        if self.header is True:
            csv.write(u",".join(col_names))
            csv.write(self.linesep)
        for irow in range(size):
            row = []
            for column in columns:
                if column[irow] is None:
                    row.append('nan')
                else:
                    row.append(column[irow])
            csv.write(u",".join(map(str, row)))
            csv.write(self.linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        columns = [pa.array(a) for a in columns]
        expected = pa.RecordBatch.from_arrays(columns, col_names)
        return csv, col_names, expected

    def generate(self):
        self.__logger.info('Generate')
        self.__logger.info("%s: Producing Data" % (self.__class__.__name__))
        self.__logger.debug("%s: Producing Data" % (self.__class__.__name__))
        print("WTFWTFWTFWTFWTF!!!!!!!!!!!!!")
        i = 0
        mysum = 0
        mysumsize = 0
        while i < self.nbatches:
            data, col_names, batch = self.make_mixed_random_csv()
            # Should be bytes.
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(data)))
            mysumsize += sys.getsizeof(data)
            mysum += len(data)
            i += 1
            yield data

        # Helped to figure out the math for an average float size.
        self.__logger.debug('%s: Average of total: %2.1f' %
                            (self.__class__.__name__, mysum/i))
        # Same as previous.
        self.__logger.debug('%s: Average of size: %2.1f' %
                            (self.__class__.__name__, mysumsize/i))
