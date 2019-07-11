# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import apache_beam as beam


class Log(beam.PTransform):

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.FlatMap(self._log)

    def _log(self, element):
        logging.info(element)
        yield element
