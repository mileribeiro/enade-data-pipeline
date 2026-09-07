trigger-pipeline:
	@aws glue start-workflow-run --name enade-pipeline

tf-apply:
	@cd tf && terraform apply