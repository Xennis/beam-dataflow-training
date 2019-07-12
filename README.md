# Apache Beam & Google Dataflow training

[![Build Status](https://travis-ci.org/Xennis/beam-dataflow-training.svg?branch=master)](https://travis-ci.org/Xennis/beam-dataflow-training)

## Setup

### Local setup

Requirements
* Python 2.7 is installed
* Google Cloud SDK is installed

Create a virtual environment and install the dependencies
```sh
virtualenv --python python2.7 .venv
. .venv/bin/activate
pip install --requirement requirements.txt
```

Login with the Google Cloud SDK
```sh
gcloud auth login
gcloud auth application-default login
```

### GCP setup

* Create a bucket
* Enable the `dataflow.googleapis.com` API

## Run

### Locally with DirectRunner

```sh
python customer.py \
    --setup_file ./setup.py \
    --detail_input ./pipeline/customer/testdata/detail.json \
    --order_input ./pipeline/customer/testdata/order.json \
    --output ./pipeline/customer/testdata/output
```

### Google Dataflow

#### Directly with the DataflowRunner

```sh
python customer.py \
    --runner DataflowRunner \
    --project dataflow-training \
    --region europe-west1 \
    --staging_location gs://dataflow-training-temp/customer/staging \
    --temp_location gs://dataflow-training-temp/temp \
    --job_name customer-orders \
    --setup_file ./setup.py \
    --detail_input gs://dataflow-training-temp/customer/testdata/detail.json \
    --order_input gs://dataflow-training-temp/customer/testdata/order.json \
    --output gs://dataflow-training-temp/customer/output/output
```

#### Compile a Dataflow template

```sh
python customer.py \
    --runner DataflowRunner \
    --project dataflow-training \
    --region europe-west1 \
    --staging_location gs://dataflow-training-persistent/customer/staging \
    --temp_location gs://dataflow-training-temp/temp \
    --template_location gs://dataflow-training-persistent/templates/customer \
    --setup_file ./setup.py \
    --detail_input gs://dataflow-training-temp/customer/testdata/detail.json \
    --order_input gs://dataflow-training-temp/customer/testdata/order.json \
    --output gs://dataflow-training-temp/customer/output/output    
```

#### Run a job from a Dataflow template

```sh
gcloud dataflow jobs run dataflow-training \
    --region europe-west1 \
    --max-workers 5 \
    --gcs-location gs://dataflow-training-persistent/templates/customer
```

Alternatively, use the Dataflow UI or one of the options at https://cloud.google.com/dataflow/docs/guides/templates/executing-templates.
