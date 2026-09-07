run:
	@aws glue start-workflow-run --name enade-pipeline --run-properties '{"YEAR":"2023"}'