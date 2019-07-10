# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import apache_beam as beam


class Log(beam.DoFn):

    def process(self, element, *args, **kwargs):
        logging.info(element)
        yield element
