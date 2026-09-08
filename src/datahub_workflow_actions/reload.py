"""Hot reload (§18.24). The engine reads its recipe once at start; editing a rule in the
builder used to need a restart, which cut off whatever was running. `RecipeWatcher`
re-reads the ingestion source's recipe from DataHub on a timer and, when the parts the
action consumes have changed, hands the new config to the action, which swaps it in
between events. A recipe that fails to parse is logged and ignored — the engine keeps
running the last good rules."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("datahub_workflow_actions.reload")

SOURCE_RECIPE_QUERY = """
query workflowActionsSourceRecipe($urn: String!) {
  ingestionSource(urn: $urn) { urn config { recipe } }
}"""

# The keys of source.config the running action consumes. Anything else (kafka, executorId,
# pipelineName, statePath, reload interval) needs a restart and is deliberately ignored here.
RELOADABLE_KEYS = ("schemaVersion", "rules", "connections", "sqlTemplates", "runHistory", "dryRun", "dedupeWindowSeconds", "limits")


def parse_recipe_text(text: str) -> Dict[str, Any]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        import yaml

        data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("recipe is not a mapping")
    return data


def source_config_of(recipe: Dict[str, Any]) -> Dict[str, Any]:
    source = recipe.get("source") if isinstance(recipe.get("source"), dict) else {}
    config = source.get("config") if isinstance(source.get("config"), dict) else {}
    return dict(config)


def fetch_source_config(graph: Any, source_urn: str) -> Optional[Dict[str, Any]]:
    """The ingestion source's current `source.config`, or None when it cannot be read."""
    result = graph.execute_graphql(SOURCE_RECIPE_QUERY, variables={"urn": source_urn})
    recipe = ((result or {}).get("ingestionSource") or {}).get("config", {}).get("recipe")
    if not recipe:
        return None
    return source_config_of(parse_recipe_text(recipe))


def reloadable_fingerprint(config: Dict[str, Any]) -> str:
    subset = {key: config.get(key) for key in RELOADABLE_KEYS if key in config}
    return hashlib.sha256(json.dumps(subset, sort_keys=True, default=str).encode("utf-8")).hexdigest()


class RecipeWatcher:
    """Polls `fetch()` every `interval_seconds`; calls `apply(config)` when the reloadable
    part changed. `check_once()` is the unit of work, usable without the thread."""

    def __init__(
        self,
        fetch: Callable[[], Optional[Dict[str, Any]]],
        apply: Callable[[Dict[str, Any]], None],
        interval_seconds: float = 30.0,
        initial: Optional[Dict[str, Any]] = None,
    ):
        self.fetch = fetch
        self.apply = apply
        self.interval = interval_seconds
        self.fingerprint = reloadable_fingerprint(initial or {})
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.reloads = 0

    def check_once(self) -> str:
        """'unchanged' | 'reloaded' | 'invalid' | 'unavailable'."""
        try:
            config = self.fetch()
        except Exception as e:  # noqa: BLE001 — a flaky read must not stop the engine
            logger.warning("workflow-actions: could not re-read the recipe: %s", e)
            return "unavailable"
        if config is None:
            return "unavailable"
        fingerprint = reloadable_fingerprint(config)
        if fingerprint == self.fingerprint:
            return "unchanged"
        try:
            self.apply(config)
        except Exception as e:  # noqa: BLE001 — keep the last good rules
            logger.warning("workflow-actions: recipe changed but could not be applied, keeping current rules: %s", e)
            return "invalid"
        self.fingerprint = fingerprint
        self.reloads += 1
        logger.info("workflow-actions: recipe reloaded (%s rule(s))", len(config.get("rules") or []))
        return "reloaded"

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self.check_once()

    def start(self) -> None:
        if self._thread is not None or self.interval <= 0:
            return
        self._thread = threading.Thread(target=self._loop, name="workflow-actions-reload", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
