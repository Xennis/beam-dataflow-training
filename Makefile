# Set the GCP project ID: 
# export GCP_PROJECT=<project-id>
GCP_REGION = europe-west1
GCP_BUCKET = dataflow-training-persistent
GCP_TEMP_BUCKET = dataflow-training-temp

runLocal:
	@echo "Run pipeline directly with the DirectRunner."
	python customer.py \
		--setup_file ./setup.py \
		--detail_input ./pipeline/customer/testdata/detail.json \
		--order_input ./pipeline/customer/testdata/order.json \
		--output ./pipeline/customer/testdata/output \
		--output_aggregation sum

runDataflow:
	@echo "Run pipeline in Dataflow."
	python customer.py \
		--runner DataflowRunner \
		--project $(GCP_PROJECT) \
		--region $(GCP_REGION) \
		--staging_location gs://$(GCP_TEMP_BUCKET)/customer/staging \
		--temp_location gs://$(GCP_TEMP_BUCKET)/temp \
		--job_name customer-orders \
		--setup_file ./setup.py \
		--detail_input gs://$(GCP_TEMP_BUCKET)/customer/testdata/detail.json \
		--order_input gs://$(GCP_TEMP_BUCKET)/customer/testdata/order.json \
		--output gs://$(GCP_TEMP_BUCKET)/customer/output/output \
		--output_aggregation avg

template:
	@echo "Compile and upload a Dataflow template"
	python customer.py \
		--runner DataflowRunner \
		--project $(GCP_PROJECT) \
		--region $(GCP_REGION) \
		--staging_location gs://$(GCP_BUCKET)/customer/staging \
		--temp_location gs://$(GCP_TEMP_BUCKET)/temp \
		--template_location gs://$(GCP_BUCKET)/templates/customer \
		--setup_file ./setup.py \
		--detail_input gs://$(GCP_TEMP_BUCKET)/customer/testdata/detail.json \
		--order_input gs://$(GCP_TEMP_BUCKET)/customer/testdata/order.json \
		--output gs://$(GCP_TEMP_BUCKET)/customer/output/output
	@echo "Upload the metadata file of the pipeline next to the template"
	gsutil cp customer_metadata gs://$(GCP_TEMP_BUCKET)/templates/

run:
	@echo "Run the template with the specified parameters"
	gcloud dataflow jobs run dataflow-training \
		--project $(GCP_PROJECT) \
		--region $(GCP_REGION) \
		--max-workers 5 \
		--gcs-location gs://$(GCP_BUCKET)/templates/customer \
		--parameters 'output_aggregation=min'
