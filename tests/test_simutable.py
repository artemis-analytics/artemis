#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Test protobuf model implementation
"""

import unittest
# import tempfile
# from collections import OrderedDict

from artemis.generators.simutable.synthesizer import Synthesizer
from artemis.io.protobuf.simutable_pb2 import SimuTable


class TestCase(unittest.TestCase):

    def test(self):
        model = SimuTable()
        model.name = 'EvolveModel'
        model.description = 'Evolution of the mind'
        field = model.fields.add()
        field.name = 'Name'
        field.type = 'String'
        field.length = 10
        field.generator.name = 'name'

        print(model)

    def test_gen_from_proto(self):

        model = SimuTable()
        model.name = 'EvolveModel'
        model.description = 'Evolution of the mind'
        field = model.fields.add()
        field.name = 'Name'
        field.type = 'String'
        field.length = 10
        field.generator.name = 'name'

        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        print(s2.generate())
        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        print(s2.generate())

    def test_glm_proto(self):
        model = SimuTable()
        field1 = model.fields.add()
        field1.name = 'Value1'
        field1.type = 'Float'
        field1.length = 10
        field1.generator.name = 'random_int'
        field1.dependent = 'Prediction'

        field2 = model.fields.add()
        field2.name = 'Value2'
        field2.type = 'Float'
        field2.length = 10
        field2.generator.name = 'random_int'
        field2.dependent = 'Prediction'

        field3 = model.fields.add()
        field3.name = 'Prediction'
        field3.type = 'Float'
        field3.length = 10
        field3.generator.name = 'glm'

        beta1 = field3.generator.parameters.add()
        beta1.name = 'beta1'
        beta1.value = 10
        beta1.type = 'int'
        beta2 = field3.generator.parameters.add()
        beta2.name = 'beta2'
        beta2.value = 0.1
        beta2.type = 'float'
        beta3 = field3.generator.parameters.add()
        beta3.name = 'beta3'
        beta3.value = 100
        beta3.type = 'int'
        sigma = field3.generator.parameters.add()
        sigma.name = 'sigma'
        sigma.value = 1
        sigma.type = 'int'

        var1 = field3.generator.parameters.add()
        var1.name = 'Value1'
        var1.type = 'Field'
        var1.variable.CopyFrom(field1)

        var2 = field3.generator.parameters.add()
        var2.name = 'Value2'
        var2.type = 'Field'
        var2.variable.CopyFrom(field2)

        s2 = Synthesizer(model, 'en_CA')
        print(s2.generate())

    def test_xduplicates(self):

        model = SimuTable()

        model.duplicate.probability = 1
        model.duplicate.distribution = 'uniform'
        model.duplicate.maximum = 1

        field1 = model.fields.add()
        field1.name = 'record_id'
        field1.type = 'String'
        field1.length = 10

        field2 = model.fields.add()
        field2.name = 'Name'
        field2.type = 'String'
        field2.length = 10
        field2.generator.name = 'name'

        field3 = model.fields.add()
        field3.name = 'UPC'
        field3.type = 'Integer'
        field3.length = 13
        field3.generator.name = 'ean'

        parm = field3.generator.parameters.add()
        parm.name = 'ndigits'
        parm.value = 13
        parm.type = 'int'

        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        print(s2.generate())

    def test_xmodifer(self):

        model = SimuTable()

        field1 = model.fields.add()
        field1.name = 'record_id'
        field1.type = 'String'
        field1.length = 10

        field2 = model.fields.add()
        field2.name = 'Name'
        field2.type = 'String'
        field2.length = 10
        field2.generator.name = 'name'

        field3 = model.fields.add()
        field3.name = 'SIN' 
        field3.type = 'String'
        field3.length = 10
        field3.generator.name = 'ssn'

        field4 = model.fields.add()
        field4.name = 'StreetNumber'
        field4.type = 'String'
        field4.length = 40
        field4.generator.name = 'building_number'

        field5 = model.fields.add()
        field5.name = 'Street'
        field5.type = 'String'
        field5.length = 40
        field5.generator.name = 'street_name'

        field6 = model.fields.add()
        field6.name = 'City'
        field6.type = 'String'
        field6.length = 40
        field6.generator.name = 'city'

        field7 = model.fields.add()
        field7.name = 'Province'
        field7.type = 'String'
        field7.length = 40
        field7.generator.name = 'province'

        field8 = model.fields.add()
        field8.name = 'PostalCode'
        field8.type = 'String'
        field8.length = 40
        field8.generator.name = 'postcode'

        field9 = model.fields.add()
        field9.name = 'DOB'
        field9.type = 'DateTime'
        field9.length = 40
        field9.generator.name = 'date'

        field10 = model.fields.add()
        field10.name = 'PhoneNum'
        field10.type = 'String'
        field10.length = 11
        field10.generator.name = 'phone_number'

        model.duplicate.probability = 1
        model.duplicate.distribution = 'uniform'
        model.duplicate.maximum = 5

        modifier = model.record_modifier

        modifier.max_modifications_in_record = 1
        modifier.max_field_modifiers = 1
        modifier.max_record_modifiers = 1

        name_mod = modifier.fields.add()
        name_mod.selection = 0.1
        name_mod.name = 'Name'
        prob = name_mod.probabilities

        prob.insert = 0.1  # insert character in field
        prob.delete = 0.1  # delete character in field
        prob.substitute = 0.1  # substitute character in field
        prob.misspell = 0.  # use mispelling dictionary
        prob.transpose = 0.1  # transpose adjacent characters
        prob.replace = 0.1  # replace with another value of same fake
        prob.swap = 0.1  # swap two words/values in field
        prob.split = 0.1  # split a field
        prob.merge = 0.1  # merge a field
        prob.nullify = 0.1  # convert to null
        prob.fill = 0.1  # fill empty field with expected type

        street_mod = modifier.fields.add()
        street_mod.selection = 0.9
        street_mod.name = 'Street'
        prob2 = street_mod.probabilities

        prob2.insert = 0.1  # insert character in field
        prob2.delete = 0.1  # delete character in field
        prob2.substitute = 0.1  # substitute character in field
        prob2.misspell = 0.  # use mispelling dictionary
        prob2.transpose = 0.1  # transpose adjacent characters
        prob2.replace = 0.1  # replace with another value of same fake
        prob2.swap = 0.1  # swap two words/values in field
        prob2.split = 0.1  # split a field
        prob2.merge = 0.1  # merge a field
        prob2.nullify = 0.1  # convert to null
        prob2.fill = 0.1  # fill empty field with expected type
        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        protorows = []
        for _ in range(10):
            protorows.append(s2.generate())
        print(protorows) 

if __name__ == '__main__':
    print('Unit Test: Faker')
    unittest.main()
    print('====================================')
