# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import apache_beam as beam


class Log(beam.PTransform):

    def __init__(self, label=None, prefix=None):
        super(Log, self).__init__(label)
        self._prefix = prefix

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.FlatMap(self._log)

    def _log(self, element):
        if self._prefix:
            logging.info("%s: %s", self._prefix, element)
        else:
            logging.info("%s", element)
        yield element
