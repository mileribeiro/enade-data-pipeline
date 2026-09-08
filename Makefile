trigger-pipeline:
	@aws glue start-workflow-run --name enade-pipeline

tf-apply:
	@cd tf && terraform apply

list-data:
	@AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url http://localhost:4566 s3 ls s3://enade-data/2023/ --recursive