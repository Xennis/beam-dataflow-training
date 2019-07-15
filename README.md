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
make runLocal
```

### Google Dataflow

Set the project ID of the GCP project

```sh
export GCP_PROJECT=<project-id>
```

Upload the test data to the GCP bucket
```sh
gsutil cp customer/testdata gs://dataflow-training-temp/customer/testdata
```

#### Directly with the DataflowRunner

```sh
make runDataflow
```

#### Compile a Dataflow template

Compile the template and upload it to the GCP bucket
```sh
make template
```

#### Run a job from a Dataflow template

```sh
make run
```

Alternatively, use the Dataflow UI or one of the options at https://cloud.google.com/dataflow/docs/guides/templates/executing-templates.
