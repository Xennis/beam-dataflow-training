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

Run locally with DirectRunner:
```sh
python customer.py \
    --setup_file ./setup.py \
    --detail_input ./pipeline/customer/testdata/detail.json
```
