# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from pipeline.customer import customer

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    customer.run()
