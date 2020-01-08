# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generates the data using faker
"""
import logging

from pprint import pformat
from faker import Faker
from artemis.externals.physt.histogram1d import Histogram1D

from artemis.logger import Logger
from artemis.generators.simutable.loader import PROVIDERS
from artemis.generators.simutable.febrlgen import Modifier


@Logger.logged
class Synthesizer(object):
    '''
    Yields row of fake data
    given a metadata model
    instance of faker needs to
    be accesible to providers
    for accessing the random number generator
    Consider as a base class
    to allow for various generators

    e.g. generate data and insert null for imputation

    # Bug
    # The modifier is not picking up the seed since it is instantiated
    # before seed is set. Needs to be investigated!
    '''

    def __init__(self, model, local, idx=0, seed=None):
        '''
        requires class model name
        '''

        self.__logger.info("Synthesizer init")
        self.__logger.debug('DEBUG Message')

        self.fake = Faker(local)
        self.__reccntr = idx
        self.add_providers()
        self.schema = []
        self.is_dependent = []
        for field in model.info.schema.info.fields:
            self.schema.append(field.name)
            if field.info.aux.dependent == '':
                self.is_dependent.append(False)
            else:
                self.is_dependent.append(True)

        if seed:
            self.set_seed(seed)

        # Cache the generator functions once
        self.generator_fcns = {}

        self.set_generators_from_proto(model)

        # Following extension for generating duplicate records
        self.__dupcntr = 0
        self.__maxdup = 0
        self.__dupdist = []  # List of duplicate counts

        self._original = []
        self.duplicate = False
        self._expect_duplicate = False
        self.nduplicate_weights = None
        self.wrg = None
        self.mod = None

        # Generator counters/stats
        self.stats = {"Total": 0,
                      "Original": 0,
                      "Duplicate": 0}
        self.h_dupdist = Histogram1D(range(10))

        if model.info.aux.HasField('duplicate'):
            self.duplicate = True
            self.duplicate_cfg = dict()
            self.duplicate_cfg['Prob_duplicate'] = \
                model.info.aux.duplicate.probability
            self.duplicate_cfg['Dist_duplicate'] = \
                model.info.aux.duplicate.distribution
            self.duplicate_cfg['Max_duplicate'] = \
                model.info.aux.duplicate.maximum

            self.nduplicate_weights = self.generate_duplicate_pdf()
            if model.info.aux.HasField('record_modifier'):
                self.mod = Modifier(self.fake,
                                    self.generator_fcns,
                                    self.schema,
                                    model.info.aux.record_modifier)

        self.__logger.info('')
        self.__logger.info('Synthesizer configured')
        self.__logger.info('Model: %s' % model)
        self.__logger.info('Schema:')
        self.__logger.info(pformat(self.schema))
        self.__logger.info('Dataset record index: %d' % idx)

        if(seed):
            self.__logger.info('Seed set: %d' % seed)

        self.__logger.info('Generate duplicate records:')
        self.__logger.info(pformat(self.duplicate))

        if(self.duplicate):
            self.__logger.info('Duplicate record probabilities')
            self.__logger.info(pformat(self.duplicate_cfg))
            self.__logger.info('Duplicate PDF')
            self.__logger.info(pformat(self.nduplicate_weights))
            self.__logger.info('Record modifier configuration')
            self.__logger.info(model.info.aux.record_modifier)

    @property
    def record_count(self):
        return self.__reccntr

    @record_count.setter
    def record_count(self, value):
        self.__reccntr = value

    @property
    def schema(self):
        return self.__schema

    @schema.setter
    def schema(self, value):
        self.__schema = list(value)

    def record_counter(self):
        self.__reccntr += 1
        self.stats['Original'] += 1

    def record_id(self):
        id = 'rec-' + str(self.record_count) + '-id'
        return id

    def set_seed(self, seed):
        self.fake.seed(seed)

    def add_providers(self):
        '''
        Add custom providers
        '''
        klasses = [provider.Provider for provider in PROVIDERS]
        for k in klasses:
            self.fake.add_provider(k)

    def get_field_parameters(self, in_parms):
        '''
        Convert field parameters to/from a message to python type
        parameters which do not contain Fields
        are converted to python type
        '''
        if len(in_parms) == 0:
            return None

        values = []
        is_msg = False
        junk_chars_list = ["[", "]", "(", ")"]
        for parm in in_parms:
            if lower(parm.type) == 'field':
                is_msg = True
                continue
            if lower(param.type) != "list"
                _type = eval(parm.type)
                value = _type(parm.value)
                values.append(value)
            elif lower(param.type) == "list":
                value_instance = param.value
                for char in junk_chars_list:
                    if char in value_instance:
                        value_instance = value_instance.replace(char, "")
                values = value_instance.split(", ")

        if is_msg is True:
            return in_parms
        elif len(values) == 1:
            return values[-1]
        else:
            return values

    def set_generators_from_proto(self, table):
        self.__logger.info("Setting Generator functions from Msg")
        for field in table.info.schema.info.fields:
            self.__logger.info("Gathering fakers %s", field.name)
            if field.info.aux.dependent != '':
                continue
            self.__logger.info('Independent field %s', field)
            parms = \
                self.get_field_parameters(field.info.aux.generator.parameters)
            fake = None
            if field.name == 'record_id':
                fake = self.record_id
            else:
                try:
                    fake = \
                        self.fake.get_formatter(field.info.aux.generator.name)
                except Exception:
                    self.__logger.error('Cannot find fake in Faker ',
                                        field.info.aux.generator.name)

            self.generator_fcns[field.name] = (fake, parms)
            self.__logger.debug(parms)
            self.__logger.debug(fake)
            self.__logger.debug(self.generator_fcns[field.name])

    def generate_duplicate_pdf(self):
        '''
        Create a map of duplicates and probabilities
        according to a pdf, i.e. uniform
        and store for re-use on each original event
        current version taken directly from FEBRL
        needs review b/c number of duplicates stored starts at 2?
        '''
        num_dup = 1
        prob_sum = 0.
        prob_list = [(num_dup, prob_sum)]
        max_dups = self.duplicate_cfg['Max_duplicate']
        uniform_val = 1.0 / float(max_dups)

        for i in range(max_dups-1):
            num_dup += 1
            prob_list.append((num_dup, uniform_val+prob_list[-1][1]))
        return prob_list

    def cache_original(self, darr):
        self._original = darr

    def reset_original(self):
        self._original = []

    def generate_original(self):
        fakers = self.schema
        self.reset_original()
        self.__logger.debug('generate_original()')
        self.__logger.debug('Event ID %d' % self.record_count)
        darr = []
        for i, fake in enumerate(fakers):
            if self.is_dependent[i] is True:
                continue
            if self.generator_fcns[fake][1] is None:
                value = self.generator_fcns[fake][0]()
                darr.append(value)
            else:
                value = \
                    self.generator_fcns[fake][0](self.generator_fcns[fake][1])
                if isinstance(value, list):
                    darr.extend(value)
                else:
                    darr.append(value)
        self.record_counter()
        self.cache_original(darr)
        return darr

    def duplicate_original(self):
        darr = []
        if self._original is None:
            self.__logger.error('Error, no original record cached')
            return darr
        elif self.__dupcntr < self.duplicate_cfg['Max_duplicate']:
            darr = self._original.copy()
            if 'rec' in darr[0]:
                darr[0] = darr[0] + '-dup-' + str(self.__dupcntr)
            if self.mod:
                self.mod.modify(darr)
            self.__dupcntr += 1
            self.stats['Duplicate'] += 1
        else:
            self.__logger.error('Error, duplicate count already reached')
        return darr

    def random_select_ndups(self):
        ind = -1
        while(self.nduplicate_weights[ind][1] > self.fake.random.random()):
            ind -= 1
        return self.nduplicate_weights[ind][0]

    def expect_duplicate(self):
        '''
        Determines whether original record will be duplicated
        Gets the maximum number of duplicated records to generate
        '''
        # Reset everything for this record
        self._expect_duplicate = False
        self.__dupcntr = 0
        self.__maxdup = 0
        # Get the probability to generate duplicate for next record
        if self.fake.random.random() < self.duplicate_cfg['Prob_duplicate']:
            self._expect_duplicate = True
            self.__maxdup = self.random_select_ndups()
        else:
            self._expect_duplicate = False
            self.__maxdup = 0

        self.__logger.debug('expect_duplicate ndups: %d', self.__maxdup)

    def generate(self):
        darr = []
        if self.duplicate is True:
            # Apply duplicate data generation
            # Ensure original record is already cached
            if self._expect_duplicate is False:
                darr = self.generate_original()
                self.expect_duplicate()
                if(logging.getLogger().isEnabledFor(logging.DEBUG)):
                    self.__logger.debug('Original record')
                    self.__logger.debug(pformat(self._original))

            elif self._expect_duplicate is True:
                if self.__dupcntr < self.__maxdup:
                    darr = self.duplicate_original()
                else:
                    # Update duplicate distribution
                    self.__logger.debug("Event %d duplicates %d",
                                        (self.stats['Total'], self.__dupcntr))
                    self.h_dupdist.fill(self.__dupcntr)
                    # clear cache
                    self.reset_original()
                    darr = self.generate_original()
                    # Get the probability to generate duplicate for next record
                    self.expect_duplicate()
            else:
                self.__logger.debug('Not generating duplicate for Event ID %d',
                                    self.record_count)
                darr = self.generate_original()
        else:
            darr = self.generate_original()

        self.stats['Total'] += 1
        if self.stats['Total'] % 10000 == 0:
            self.__logger.info("Event counter: %d" % self.stats['Total'])
        self.__logger.debug('Complete Event ID %d' % self.record_count)
        return darr

    def plots(self):
        self.__logger.info("=============================================")
        self.__logger.info("Synthesizer job summary")
        self.__logger.info("=============================================")
        self.__logger.info("Duplicate distribution")
        self.__logger.info(pformat(self.__dupdist))
        self.__logger.info('Event counters')
        self.__logger.info('Total records: %d' % self.stats['Total'])
        self.__logger.info('Original records: %d' % self.stats['Original'])
        self.__logger.info('Duplicate records: %d' % self.stats['Duplicate'])
        self.__logger.info(pformat(self.h_dupdist.to_dict()))
        if self.duplicate is True:
            self.mod.get_stats()
        self.__logger.info("=============================================")
        self.__logger.info("=============================================")
