setup:
	@python3 -m venv .venv
	@.venv/bin/python -m pip install --upgrade pip
	@.venv/bin/python -m pip install -r requirements.txt

trigger-pipeline:
	@aws glue start-workflow-run --name enade-pipeline

tf-apply:
	@cd tf && terraform apply
