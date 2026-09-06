.PHONY: artifacts test
artifacts:
	python3 -c "import json; from datahub_workflow_actions.contract import rules_json_schema; print(json.dumps(rules_json_schema(), indent=2))" > contracts/rules.v1.schema.json
	python3 -c "import json; from datahub_workflow_actions.steps import catalog; print(json.dumps(catalog(), indent=2))" > contracts/catalog.json
test:
	python3 -m pytest -q
