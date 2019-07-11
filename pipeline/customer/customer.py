# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from pipeline.common import common
from pipeline.customer import detail, order


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--detail_input',
        type=str,
        help='Input file pattern for the customer details',
        required=True)
    parser.add_argument(
        '--order_input',
        type=str,
        help='Input file pattern for the customer orders',
        required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # Save the main session that defines global import, functions and variables. Otherwise they are not saved during
    # the serialization.
    # Details see https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        # pylint: disable=expression-not-assigned
        detail_valid, detail_broken = (p | 'detail' >> detail.Prepare(known_args.detail_input))
        order_valid, order_broken = (p | 'order' >> order.Prepare(known_args.order_input))

        detail_broken | 'broken_details' >> common.Log(prefix="Broken Details")
        order_broken | 'broken_orders' >> common.Log(prefix="Broken Orders")

        aggregated_orders = (
            order_valid
            | 'orders_by_customer' >> order.GroupByCustomer()
            | 'aggregate_orders' >> beam.ParDo(order.AggregateOrders())
        )

        joined = (
            {
                'detail': detail_valid,
                'order': aggregated_orders,
            }
            | beam.CoGroupByKey()
        )

        joined | 'joined_log' >> common.Log(prefix="Joined Output")
