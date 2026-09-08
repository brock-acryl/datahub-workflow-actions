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


def cmd_triggers(args: argparse.Namespace) -> int:
    """§21: the EntityChangeEvent vocabulary GMS emits (categories, operations, parameter keys)."""
    from datahub_workflow_actions.contract import triggers_catalog

    print(json.dumps(triggers_catalog(), indent=2))
    return 0


def trigger_warnings(config) -> list:
    """Advice, not errors: categories / operations / entity types outside the published vocabulary."""
    from datahub_workflow_actions.contract import KNOWN_ENTITY_TYPES, KNOWN_TRIGGERS

    known_types = {t.lower() for t in KNOWN_ENTITY_TYPES}
    warnings = []
    for rule in config.event_rules():
        known = KNOWN_TRIGGERS.get(rule.on.category)
        if known is None:
            warnings.append(f"{rule.id}: category '{rule.on.category}' is not one GMS is known to emit")
            continue
        for op in rule.on.operations:
            if op not in known["operations"]:
                warnings.append(f"{rule.id}: {rule.on.category} is not known to emit operation '{op}'")
        for entity_type in rule.on.entityTypes:
            if entity_type.lower() not in known_types:
                warnings.append(f"{rule.id}: unknown entity type '{entity_type}'")
    return warnings


def cmd_validate(args: argparse.Namespace) -> int:
    try:
        raw = _load(args.rules)
        config = load_rules(raw)
    except Exception as e:  # noqa: BLE001
        print(f"INVALID: {e}", file=sys.stderr)
        return 1
    for warning in trigger_warnings(config):
        print(f"WARNING: {warning}", file=sys.stderr)
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
    connection_problems = _connection_problems(raw, config)
    if connection_problems:
        print("INVALID:\n  " + "\n  ".join(connection_problems), file=sys.stderr)
        return 1
    print(
        f"OK: {len(config.rules)} rule(s) — {len(config.workflow_rules())} workflow, {len(config.event_rules())} event; schemaVersion {config.schemaVersion}"
    )
    return 0


def _raw_source_config(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return {}
    source_config = (raw.get("source") or {}).get("config") if isinstance(raw.get("source"), dict) else None
    return source_config if isinstance(source_config, dict) else raw


def _connection_problems(raw: Any, config: RulesConfig) -> list:
    """Connection specs must be well-formed, and — when any are declared — every sql step must name one of them."""
    from datahub_workflow_actions.connections import parse_connections, validate_connections

    source_config = _raw_source_config(raw)
    declared = source_config.get("connections")
    problems = list(validate_connections(declared))
    names = set(parse_connections(declared)) if declared else None
    for rule in config.rules:
        for step in rule.steps:
            if step.type != "sql":
                continue
            name = (step.params or {}).get("connection")
            if names is not None and name and name not in names:
                problems.append(f"{rule.id}/{step.id}: connection '{name}' is not declared under connections ({', '.join(sorted(names)) or 'none'})")
    return problems


def cmd_simulate(args: argparse.Namespace) -> int:
    config = load_rules(_load(args.rules))
    event = _load(args.event)
    fixtures = _load(args.fixtures) if args.fixtures else {}
    context = build_context(event, StaticResolver(fixtures))
    if "workflow" not in context:  # §21 event rules: the own-actor guard compares against this
        context["engine"] = {"actor": args.own_actor}
    graph: Optional[Any] = None
    if args.execute:
        from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig

        graph = DataHubGraph(DatahubClientConfig(server=args.gms, token=args.token))
    from datahub_workflow_actions.action import normalize_connections
    from datahub_workflow_actions.connections import ConnectionResolver

    raw_connections = _raw_source_config(_load(args.rules)).get("connections")
    run_context = RunContext(
        graph=graph,
        connections=normalize_connections(raw_connections),
        connection_resolver=ConnectionResolver.from_config(raw_connections, graph=graph),
    )
    engine = Engine(run_context, dry_run=not args.execute)
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
    sub.add_parser("triggers", help="print the change-event vocabulary (categories, operations, parameters)").set_defaults(func=cmd_triggers)
    validate = sub.add_parser("validate", help="validate a recipe or rules file")
    validate.add_argument("rules")
    validate.set_defaults(func=cmd_validate)
    simulate = sub.add_parser("simulate", help="evaluate rules against an event (dry run unless --execute)")
    simulate.add_argument("--rules", required=True)
    simulate.add_argument("--event", required=True)
    simulate.add_argument("--fixtures", help="JSON/YAML map of urn → GraphQL-shaped fixture used to resolve the context")
    simulate.add_argument("--show-context", action="store_true")
    simulate.add_argument("--own-actor", help="urn the engine writes as; events by this actor are skipped by rules with ignoreOwnChanges")
    simulate.add_argument("--execute", action="store_true", help="really run the steps against --gms")
    simulate.add_argument("--gms", default="http://localhost:8080")
    simulate.add_argument("--token")
    simulate.set_defaults(func=cmd_simulate)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
