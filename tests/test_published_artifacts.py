import json
from pathlib import Path

from datahub_workflow_actions.contract import rules_json_schema
from datahub_workflow_actions.steps import catalog

ROOT = Path(__file__).resolve().parents[1] / "contracts"


def test_committed_schema_matches_generated():
    committed = json.loads((ROOT / "rules.v1.schema.json").read_text())
    assert committed == rules_json_schema(), "run `make artifacts` to refresh contracts/rules.v1.schema.json"


def test_committed_catalog_matches_generated():
    committed = json.loads((ROOT / "catalog.json").read_text())
    assert committed == catalog(), "run `make artifacts` to refresh contracts/catalog.json"
