# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import typing  # pylint: disable=unused-import

import shutil
import tempfile
import unittest
import warnings
from apache_beam.testing.util import open_shards

# open_shards is a future feature of Beam that we want to use for the tests.
warnings.simplefilter(action='ignore', category=FutureWarning)


class TempDir(object):

    def __init__(self):
        self.temp_dir = None

    def __enter__(self):
        self.temp_dir = tempfile.mkdtemp()
        return self.temp_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.temp_dir)


class FileTestCase(unittest.TestCase):

    def assertFileEqual(self, filename, expected, encoding='utf-8'):
        # type: (typing.Text, list, typing.Optional[typing.Text]) -> None
        actual = []
        with open_shards(filename, encoding=encoding) as f:
            for line in f:
                actual.append(line)
        self.assertEqual(expected, actual, msg='file \'{}\' has wrong content'.format(filename))
