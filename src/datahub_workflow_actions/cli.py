"""``workflow-actions`` CLI: publish the schema/catalog, validate a recipe,
simulate an event against rules (dry run by default)."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from typing import Any, Optional

import yaml

from datahub_workflow_actions.context import StaticResolver, build_context
from datahub_workflow_actions.contract import load_rules, rules_json_schema
from datahub_workflow_actions.engine import Engine
from datahub_workflow_actions.steps import RunContext, catalog


def _load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return yaml.safe_load(text)


def cmd_schema(args: argparse.Namespace) -> int:
    print(json.dumps(rules_json_schema(), indent=2))
    return 0


def cmd_catalog(args: argparse.Namespace) -> int:
    print(json.dumps(catalog(), indent=2))
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    try:
        config = load_rules(_load(args.rules))
    except Exception as e:  # noqa: BLE001
        print(f"INVALID: {e}", file=sys.stderr)
        return 1
    unknown = []
    from datahub_workflow_actions.steps import get_step, known_step_types

    known = set(known_step_types())
    for rule in config.rules:
        for step in rule.steps:
            if step.type not in known:
                unknown.append(f"{rule.id}/{step.id}: unknown step type '{step.type}'")
                continue
            hook = get_step(step.type).validate_template
            for problem in hook(step.params) if hook else []:
                unknown.append(f"{rule.id}/{step.id}: {problem}")
    if unknown:
        print("INVALID:\n  " + "\n  ".join(unknown), file=sys.stderr)
        return 1
    print(f"OK: {len(config.rules)} rule(s), schemaVersion {config.schemaVersion}")
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    config = load_rules(_load(args.rules))
    event = _load(args.event)
    fixtures = _load(args.fixtures) if args.fixtures else {}
    context = build_context(event, StaticResolver(fixtures))
    graph: Optional[Any] = None
    if args.execute:
        from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig

        graph = DataHubGraph(DatahubClientConfig(server=args.gms, token=args.token))
    from datahub_workflow_actions.action import normalize_connections

    raw_config = _load(args.rules)
    source_config = (raw_config.get("source") or {}).get("config") if isinstance(raw_config, dict) else None
    connections = normalize_connections((source_config or raw_config or {}).get("connections") if isinstance(raw_config, dict) else None)
    engine = Engine(RunContext(graph=graph, connections=connections), dry_run=not args.execute)
    runs = engine.run(config, context)
    output = {"context": context if args.show_context else None, "runs": [r.to_dict() for r in runs]}
    if not args.show_context:
        output.pop("context")
    print(json.dumps(output, indent=2, default=str))
    return 0 if all(r.status in ("ok", "dry-run", "not-fired") for r in runs) else 2


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(prog="workflow-actions", description="DataHub workflow-actions tooling")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("schema", help="print the rules JSON Schema").set_defaults(func=cmd_schema)
    sub.add_parser("catalog", help="print the step catalog").set_defaults(func=cmd_catalog)
    validate = sub.add_parser("validate", help="validate a recipe or rules file")
    validate.add_argument("rules")
    validate.set_defaults(func=cmd_validate)
    simulate = sub.add_parser("simulate", help="evaluate rules against an event (dry run unless --execute)")
    simulate.add_argument("--rules", required=True)
    simulate.add_argument("--event", required=True)
    simulate.add_argument("--fixtures", help="JSON/YAML map of urn → GraphQL-shaped fixture used to resolve the context")
    simulate.add_argument("--show-context", action="store_true")
    simulate.add_argument("--execute", action="store_true", help="really run the steps against --gms")
    simulate.add_argument("--gms", default="http://localhost:8080")
    simulate.add_argument("--token")
    simulate.set_defaults(func=cmd_simulate)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
