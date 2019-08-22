#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.
import logging


class AbcAlgoBase(type):
    '''
    https://stackoverflow.com/questions/29069655/python-logging-with-a-common-logger-class-mixin-and-class-inheritance

    Logger for the Base class and each derived class.
    Not for instances though
    To identify logging from different configurations
    pass the instance name (attribute)
    '''
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Explicit name mangling
        logger_attribute_name = '_' + cls.__name__ + '__logger'

        # Logger name derived accounting for inheritance for the bonus marks
        # Combining the Mixins and base classes,
        # the naming is convenient for the logging
        # logger_name = '.'.join([c.__name__ for c in cls.mro()[-2::-1]])

        logger_name = cls.__name__
        def fget(cls): return getattr(cls, logger_attribute_name)

        # add the getter property to cls
        setattr(cls, 'logger', property(fget))
        # add the logger to cls
        setattr(cls, logger_attribute_name, logging.getLogger(logger_name))
