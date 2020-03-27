#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""


class NullDataError(ValueError):
    """
    """


class MissingDataError(ValueError):
    """
    """


class ParserWarning(Warning):
    """
    """


class AbstractMethodError(NotImplementedError):
    """
    Pandas errors.py
    Raise this error instead of NotImplementedError
    """

    def __init__(self, class_instance, methodtype="method"):
        types = {"method", "classmethod", "staticmethod", "property"}
        if methodtype not in types:
            msg = "methodtype must be one of {}, got {} instead.".format(
                methodtype, types
            )
            raise ValueError(msg)
        self.methodtype = methodtype
        self.class_instance = class_instance

    def __str__(self):
        if self.methodtype == "classmethod":
            name = self.class_instance.__name__
        else:
            name = self.class_instance.__class__.__name__
        msg = "This {methodtype} must be defined in the concrete class {name}"
        return msg.format(methodtype=self.methodtype, name=name)
