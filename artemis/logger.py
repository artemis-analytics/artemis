#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Logger class

References:
github.com/iteration/dvc
https://docs.python.org/3/howto/logging-cookbook.html


Notes:
Single Logger to handle msgs based on LogLevel
checks against log level before formatting.

if(logging.getLogger().isEnabledFor(logging.DEBUG)):
        self.log.debug('Original record')
        self.log.debug(pformat(self._original))

Automatically create loggers for every algorithm
Set level for clt.

Need loggers to indicate module or class.
Class below may not work???
"""

import logging
import argparse
import sys
# import traceback


_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']


def _log_level_string_to_int(log_level_string):
    if not log_level_string in _LOG_LEVEL_STRINGS:
        message = 'invalid choice: {0} (choose from {1})'.format(log_level_string, _LOG_LEVEL_STRINGS)
        raise argparse.ArgumentTypeError(message)

    log_level_int = getattr(logging, log_level_string, logging.INFO)
                            
    assert isinstance(log_level_int, int)

    return log_level_int


class Logger():
    FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    DEFAULT_LEVEL = logging.INFO

    def __init__(self, loglevel=None):
        if loglevel:
            Logger.set_level(loglevel)

    @staticmethod
    def init():
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(Logger.FMT))
        sh.setLevel(logging.DEBUG)

        logging.getLogger('transitions').setLevel(logging.DEBUG)

        Logger.logger().addHandler(sh)
        Logger.set_level()
    
    @staticmethod
    def set_instance():
        '''
        set the return logger instance
        '''
        pass

    @staticmethod
    def logger():
        '''
        Use a decorator to getLogger with module name?
        '''
        return logging.getLogger('artemis')

    def set_level(level=None):
        if not level:
            lvl = Logger.DEFAULT_LEVEL
        else:
            lvl = _log_level_string_to_int

        Logger.logger().setLevel(lvl)   

    @staticmethod
    def _prefix(msg, typ):
        return '{}: {}'.format(typ, msg)

    @staticmethod
    def error_prefix():
        return Logger._prefix('ERROR', 'error')
    
    @staticmethod
    def debug_prefix():
        return Logger._prefix('DEBUG', 'debug')
    
    @staticmethod
    def info_prefix():
        return Logger._prefix('INFO', 'debug')

    @staticmethod
    def warn(msg):
        return Logger.logger().warn(Logger.warning_prefix() + msg)

    @staticmethod
    def debug(msg):
        return Logger.logger().debug(Logger.debug_prefix() + msg)

    @staticmethod
    def info(msg):
        return Logger.logger().info(msg)

