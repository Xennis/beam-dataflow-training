# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal
import json
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


class PipelineJSONEncoder(json.JSONEncoder):
    """PipelineJSONEncoder is a JSONEncoder with some special encoding rules for some data types."""

    def __init__(self, skipkeys=False, ensure_ascii=True, check_circular=True,
                 allow_nan=False, indent=None, separators=None,
                 encoding='utf-8', default=None, sort_keys=False):
        super(PipelineJSONEncoder, self).__init__(skipkeys=skipkeys, ensure_ascii=ensure_ascii,
                                                  check_circular=check_circular, allow_nan=allow_nan, indent=indent,
                                                  separators=separators, encoding=encoding, default=default,
                                                  sort_keys=sort_keys)

    # pylint: disable=method-hidden
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(PipelineJSONEncoder, self).default(o)
