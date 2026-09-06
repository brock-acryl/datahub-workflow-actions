"""Run history: every rule that fires is recorded in DataHub as a run, the same
model Airflow/dbt runs use, so the MFE (and DataHub itself) can show what an
action did to which asset and whether it worked.

Shape (all deterministic, so the MFE can address them without lookups):

* DataFlow  ``urn:li:dataFlow:(workflow-actions,<workflow id>,PROD)``  — one per workflow
* DataJob   ``urn:li:dataJob:(<flow urn>,<rule id>)``                   — one per rule
* DataProcessInstance — one per fire; STARTED then COMPLETE with SUCCESS/FAILURE,
  properties carry the request, requester, entity and a compact step report.

Only rules that actually fired are recorded; dry runs are skipped unless
``recordDryRuns`` is set. Recording is best-effort — a failure to write history
never fails the action."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger("datahub_workflow_actions.runs")

ORCHESTRATOR = "workflow-actions"
ENV = "PROD"
# Custom-property keys the MFE reads back (kept short and stable — they're the contract).
PROP_RULE_ID = "ruleId"
PROP_RULE_NAME = "ruleName"
PROP_WORKFLOW_URN = "workflowUrn"
PROP_REQUEST_URN = "requestUrn"
PROP_REQUESTER_URN = "requesterUrn"
PROP_ENTITY_URN = "entityUrn"
PROP_OPERATION = "operation"
PROP_RESULT = "result"
PROP_STATUS = "status"
PROP_REASON = "reason"
PROP_STEPS = "steps"
MAX_STEPS_JSON = 20000  # keep the properties aspect small; outputs are truncated first


def workflow_id(workflow_urn: str) -> str:
    return workflow_urn.rsplit(":", 1)[-1] if workflow_urn else "unknown"


def flow_urn(workflow_urn: str) -> str:
    return f"urn:li:dataFlow:({ORCHESTRATOR},{workflow_id(workflow_urn)},{ENV})"


def job_urn(workflow_urn: str, rule_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn(workflow_urn)},{rule_id})"


def run_id(rule_id: str, request_urn: str, operation: str, result: str, event_time_ms: int) -> str:
    raw = f"{rule_id}|{request_urn}|{operation}|{result}|{event_time_ms}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def run_urn(rule_id: str, request_urn: str, operation: str, result: str, event_time_ms: int) -> str:
    """The SDK derives the instance urn from (orchestrator, cluster, id); reuse it so callers agree."""
    from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance

    return str(DataProcessInstance(id=run_id(rule_id, request_urn, operation, result, event_time_ms), orchestrator=ORCHESTRATOR, cluster=ENV).urn)


def _truncate(value: Any, limit: int = 500) -> Any:
    if isinstance(value, str) and len(value) > limit:
        return value[: limit - 1] + "…"
    if isinstance(value, dict):
        return {k: _truncate(v, limit) for k, v in list(value.items())[:50]}
    if isinstance(value, list):
        return [_truncate(v, limit) for v in value[:50]]
    return value


def steps_report(steps: List[Any]) -> str:
    """Compact JSON the MFE renders as the step-by-step outcome."""
    rows = []
    for step in steps:
        row = {
            "id": step.stepId,
            "type": step.type,
            "status": step.status,
            "attempts": step.attempts,
        }
        if step.reason:
            row["reason"] = step.reason
        if step.error:
            row["error"] = _truncate(step.error, 1000)
        if step.output is not None:
            row["output"] = _truncate(step.output)
        if step.items:
            row["items"] = [{"status": i.status, "error": _truncate(i.error, 300)} for i in step.items[:50]]
        rows.append(row)
    text = json.dumps(rows, default=str, separators=(",", ":"))
    if len(text) > MAX_STEPS_JSON:
        rows = [{k: v for k, v in r.items() if k != "output"} for r in rows]
        text = json.dumps(rows, default=str, separators=(",", ":"))
    return text[:MAX_STEPS_JSON]


@dataclass
class RunRecorderConfig:
    enabled: bool = True
    recordDryRuns: bool = False


class RunRecorder:
    """Writes flow/job/instance MCPs through a DataHubGraph (or any emitter with emit_mcp)."""

    def __init__(self, emitter: Any, config: Optional[RunRecorderConfig] = None):
        self.emitter = emitter
        self.config = config or RunRecorderConfig()
        self._templates_emitted: set = set()

    @classmethod
    def from_config(cls, emitter: Any, raw: Optional[Mapping[str, Any]]) -> Optional["RunRecorder"]:
        raw = raw or {}
        cfg = RunRecorderConfig(enabled=bool(raw.get("enabled", True)), recordDryRuns=bool(raw.get("recordDryRuns", False)))
        if emitter is None or not cfg.enabled:
            return None
        return cls(emitter, cfg)

    # -- public ---------------------------------------------------------------

    def record(self, run: Any, rule: Any, context: Mapping[str, Any], *, started_ms: Optional[int] = None) -> Optional[str]:
        """Records one RuleRun; returns the instance urn, or None when not recorded."""
        if not run.fired:
            return None
        if run.status == "dry-run" and not self.config.recordDryRuns:
            return None
        try:
            return self._record(run, rule, context, started_ms or int(time.time() * 1000))
        except Exception as e:  # noqa: BLE001 — history must never break the action
            logger.warning("workflow-actions: could not record run history for rule %s: %s", run.ruleId, e)
            return None

    # -- internals ------------------------------------------------------------

    def _record(self, run: Any, rule: Any, context: Mapping[str, Any], started_ms: int) -> str:
        from datahub.api.entities.datajob import DataFlow, DataJob
        from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance, InstanceRunResult
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn

        event = context.get("event") or {}
        workflow = context.get("workflow") or {}
        request = context.get("request") or {}
        requester = context.get("requester") or {}
        entity = context.get("entity") or {}
        wf_urn = str(workflow.get("urn") or getattr(rule, "workflowUrn", "") or "")
        request_urn = str(request.get("urn") or event.get("entityUrn") or "")
        operation = str(event.get("operation") or "")
        result = str(event.get("result") or "")
        event_time = int(event.get("time") or started_ms)

        flow = DataFlow(
            id=workflow_id(wf_urn),
            orchestrator=ORCHESTRATOR,
            env=ENV,
            name=str(workflow.get("name") or workflow_id(wf_urn)),
            description="Automations run by DataHub Workflow Actions when requests through this workflow are decided.",
            properties={PROP_WORKFLOW_URN: wf_urn},
        )
        job = DataJob(
            id=run.ruleId,
            flow_urn=DataFlowUrn.from_string(flow_urn(wf_urn)),
            name=str(getattr(rule, "name", None) or run.ruleId),
            description=str(getattr(rule, "description", None) or ""),
            properties={PROP_RULE_ID: run.ruleId, PROP_WORKFLOW_URN: wf_urn},
        )
        template_key = job_urn(wf_urn, run.ruleId)
        if template_key not in self._templates_emitted:
            flow.emit(self.emitter)
            job.emit(self.emitter)
            self._templates_emitted.add(template_key)

        entity_urn = str(entity.get("urn") or "")
        inlets = [DatasetUrn.from_string(entity_urn)] if entity_urn.startswith("urn:li:dataset:") else []
        instance = DataProcessInstance(
            id=run_id(run.ruleId, request_urn, operation, result, event_time),
            orchestrator=ORCHESTRATOR,
            cluster=ENV,
            type="BATCH_AD_HOC",
            template_urn=DataJobUrn.from_string(template_key),
            inlets=inlets,
            properties={
                PROP_RULE_ID: run.ruleId,
                PROP_RULE_NAME: str(getattr(rule, "name", None) or run.ruleId),
                PROP_WORKFLOW_URN: wf_urn,
                PROP_REQUEST_URN: request_urn,
                PROP_REQUESTER_URN: str(requester.get("urn") or ""),
                PROP_ENTITY_URN: entity_urn,
                PROP_OPERATION: operation,
                PROP_RESULT: result,
                PROP_STATUS: run.status,
                PROP_REASON: str(run.reason or ""),
                PROP_STEPS: steps_report(run.steps),
            },
        )
        instance.emit_process_start(self.emitter, started_ms, emit_template=False, materialize_iolets=False)
        ok = run.status in ("ok", "dry-run")
        instance.emit_process_end(
            self.emitter,
            int(time.time() * 1000),
            result=InstanceRunResult.SUCCESS if ok else InstanceRunResult.FAILURE,
            result_type=run.status,
            start_timestamp_millis=started_ms,
        )
        return str(instance.urn)
