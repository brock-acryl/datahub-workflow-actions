"""Connections for the ``sql`` step — declared once under ``connections`` in the
action config as ``name → spec`` and referenced by name from rules.

Three kinds, mirroring how DataHub Cloud assertions obtain credentials (the
executor turns an ingestion source's recipe into a connection at run time):

* ``{url: "postgresql://u:${SECRET}@host/db"}`` (or a bare string) — a
  SQLAlchemy URL. ``${VAR}`` placeholders resolve from the environment; when
  the source runs inside the DataHub executor they were already substituted
  from DataHub secrets before the recipe reached us.
* ``{ingestionSource: "urn:li:dataHubIngestionSource:…"}`` — reuse that
  source's recipe: fetch it via GraphQL, resolve its ``${SECRETS}`` through the
  DataHub secret stores, and build a SQLAlchemy URL from the platform config.
* ``{fromEntity: true}`` — like the above, but the source is whichever one
  produced the event's entity (``ingestionSourceForEntity``), so one rule can
  serve every table of a platform.

Extra keys (``platform``, ``description``, anything the MFE adds) are kept and
ignored here."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import quote_plus, urlencode, urlparse

logger = logging.getLogger("datahub_workflow_actions.connections")

INGESTION_SOURCE_URN_PREFIX = "urn:li:dataHubIngestionSource:"
CLI_EXECUTOR_ID = "__datahub_cli_"
_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

INGESTION_SOURCE_QUERY = """query ingestionSource($urn: String!) {
  ingestionSource(urn: $urn) { urn name type config { recipe executorId } }
}"""

INGESTION_SOURCE_FOR_ENTITY_QUERY = """query ingestionSourceForEntity($urn: String!) {
  ingestionSourceForEntity(urn: $urn) { urn name type config { recipe executorId } }
}"""


class ConnectionError_(ValueError):
    """Raised when a connection can't be resolved; the message is user-facing."""


@dataclass
class ConnectionSpec:
    name: str
    kind: str  # url | ingestionSource | fromEntity
    url: Optional[str] = None
    ingestion_source: Optional[str] = None
    platform: Optional[str] = None
    description: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def problems(self) -> List[str]:
        out: List[str] = []
        if self.kind == "url":
            if not self.url:
                out.append(f"connection '{self.name}': url is required")
            elif not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", self.url):
                out.append(f"connection '{self.name}': url must start with a SQLAlchemy scheme (e.g. postgresql://)")
            elif url_has_literal_password(self.url):
                out.append(f"connection '{self.name}': the password must be a ${{SECRET}} reference, not a literal value")
        elif self.kind == "ingestionSource":
            if not (self.ingestion_source or "").startswith(INGESTION_SOURCE_URN_PREFIX):
                out.append(f"connection '{self.name}': ingestionSource must be a dataHubIngestionSource urn")
        elif self.kind != "fromEntity":
            out.append(f"connection '{self.name}': unknown kind {self.kind}")
        return out


def url_has_literal_password(url: str) -> bool:
    match = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://([^/@]*)@", url)
    if not match or ":" not in match.group(1):
        return False
    password = match.group(1).split(":", 1)[1]
    return bool(password) and not re.fullmatch(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}", password)


def parse_connections(raw: Optional[Mapping[str, Any]]) -> Dict[str, ConnectionSpec]:
    """``{name: url | {url} | {ingestionSource} | {fromEntity: true}}`` → specs. Unknown shapes are skipped with a warning."""
    specs: Dict[str, ConnectionSpec] = {}
    for name, value in (raw or {}).items():
        name = str(name)
        if isinstance(value, str):
            specs[name] = ConnectionSpec(name=name, kind="url", url=value, raw={"url": value})
            continue
        if not isinstance(value, Mapping):
            logger.warning("workflow-actions: connection %r has an unsupported shape (%s); skipped", name, type(value).__name__)
            continue
        common = {"platform": value.get("platform"), "description": value.get("description"), "raw": dict(value)}
        if value.get("fromEntity") is True:
            specs[name] = ConnectionSpec(name=name, kind="fromEntity", **common)
        elif "ingestionSource" in value:
            specs[name] = ConnectionSpec(name=name, kind="ingestionSource", ingestion_source=value.get("ingestionSource"), **common)
        else:
            specs[name] = ConnectionSpec(name=name, kind="url", url=value.get("url"), **common)
    return specs


def validate_connections(raw: Optional[Mapping[str, Any]]) -> List[str]:
    return [p for spec in parse_connections(raw).values() for p in spec.problems()]


def substitute_env(url: str, env: Optional[Mapping[str, str]] = None) -> str:
    environ = os.environ if env is None else env
    return _VAR.sub(lambda m: str(environ.get(m.group(1), m.group(0))), url)


def dialect_of(url: str) -> str:
    return url.split(":", 1)[0].split("+", 1)[0].lower() if url else "ansi"


@dataclass
class ResolvedConnection:
    name: str
    kind: str
    url: str
    dialect: str
    engine_kwargs: Dict[str, Any] = field(default_factory=dict)
    source_urn: Optional[str] = None
    source_type: Optional[str] = None

    def describe(self) -> Dict[str, Any]:
        """Dry-run friendly: never includes the URL (it may carry a resolved secret)."""
        out: Dict[str, Any] = {"connection": self.name, "kind": self.kind, "dialect": self.dialect}
        if self.source_urn:
            out["ingestionSource"] = self.source_urn
        if self.source_type:
            out["sourceType"] = self.source_type
        return out


# ---------------------------------------------------------------------------
# Recipe → SQLAlchemy URL, per ingestion source type
# ---------------------------------------------------------------------------

_SCHEMES = {
    "postgres": "postgresql+psycopg2",
    "redshift": "redshift+psycopg2",
    "mysql": "mysql+pymysql",
    "mariadb": "mysql+pymysql",
    "mssql": "mssql+pymssql",
    "oracle": "oracle+cx_oracle",
    "trino": "trino",
    "presto": "presto",
    "clickhouse": "clickhouse",
    "hive": "hive",
    "vertica": "vertica+vertica_python",
    "teradata": "teradatasql",
    "sqlite": "sqlite",
}


def _auth(username: Optional[str], password: Optional[str]) -> str:
    if not username and not password:
        return ""
    user = quote_plus(str(username)) if username else ""
    return f"{user}:{quote_plus(str(password))}@" if password else f"{user}@"


def url_from_recipe(source_type: str, config: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Builds ``(url, create_engine kwargs)`` from an ingestion source's ``source.config``.

    Prefers the ingestion library's own config model when importable (same
    class the executor uses for assertions), otherwise composes the URL from
    the documented recipe fields."""
    source_type = source_type.lower()
    if source_type == "snowflake":
        return _snowflake_url(config), {}
    if source_type in ("unity-catalog", "databricks"):
        return _databricks_url(config), {}
    if source_type in ("bigquery", "bigquery-v2"):
        return _bigquery_url(config)
    if source_type in _SCHEMES:
        return _sqlalchemy_url(_SCHEMES[source_type], config), {}
    raise ConnectionError_(f"ingestion source type '{source_type}' can't be turned into a SQL connection")


def _snowflake_url(config: Mapping[str, Any]) -> str:
    try:  # library first: handles authenticator/private key/host overrides
        from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnectionConfig  # type: ignore

        return str(SnowflakeConnectionConfig.parse_obj_allow_extras(dict(config)).get_sql_alchemy_url())
    except Exception:  # noqa: BLE001 — fall back to composing from fields
        pass
    account = config.get("account_id") or config.get("host_port")
    if not account:
        raise ConnectionError_("snowflake recipe has no account_id")
    params = {k: v for k, v in (("warehouse", config.get("warehouse")), ("role", config.get("role"))) if v}
    database = config.get("database")
    return f"snowflake://{_auth(config.get('username'), config.get('password'))}{account}{'/' + quote_plus(str(database)) if database else ''}{'?' + urlencode(params) if params else ''}"


def _databricks_url(config: Mapping[str, Any]) -> str:
    workspace = str(config.get("workspace_url") or "")
    host = urlparse(workspace).netloc or workspace.replace("https://", "").rstrip("/")
    token = config.get("token")
    if not host or not token:
        raise ConnectionError_("unity-catalog recipe needs workspace_url and token")
    warehouse_id = config.get("warehouse_id") or (config.get("profiling") or {}).get("warehouse_id")
    if not warehouse_id:
        raise ConnectionError_("unity-catalog recipe needs a warehouse_id (SQL warehouse) to run statements")
    params = {"http_path": f"/sql/1.0/warehouses/{warehouse_id}"}
    return f"databricks://token:{quote_plus(str(token))}@{host}?{urlencode(params)}"


def _bigquery_url(config: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    project = config.get("project_on_behalf") or (config.get("project_ids") or [None])[0] or config.get("project_id")
    credential = config.get("credential")
    kwargs: Dict[str, Any] = {}
    if isinstance(credential, Mapping) and credential:
        info = dict(credential)
        info.setdefault("type", "service_account")
        kwargs["credentials_info"] = info
        project = project or info.get("project_id")
    return f"bigquery://{project}" if project else "bigquery://", kwargs


def _sqlalchemy_url(scheme: str, config: Mapping[str, Any]) -> str:
    if config.get("sqlalchemy_uri"):
        return str(config["sqlalchemy_uri"])
    host_port = config.get("host_port")
    if not host_port:
        raise ConnectionError_("recipe has neither sqlalchemy_uri nor host_port")
    database = config.get("database")
    options = config.get("options") or {}
    query = options.get("connect_args") if isinstance(options, Mapping) else None
    return f"{scheme}://{_auth(config.get('username'), config.get('password'))}{host_port}{'/' + quote_plus(str(database)) if database else ''}{'?' + urlencode(query) if isinstance(query, Mapping) and query else ''}"


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


def _secret_stores(graph: Any) -> list:
    """DataHub UI secrets (via the graph's token) → mounted files → environment; same precedence as the executor."""
    stores: list = []
    try:
        from datahub.secret.datahub_secret_store import DataHubSecretStore, DataHubSecretStoreConfig  # type: ignore

        if graph is not None:
            stores.append(DataHubSecretStore(DataHubSecretStoreConfig(graph_client=graph)))
    except Exception as e:  # noqa: BLE001
        logger.debug("workflow-actions: DataHub secret store unavailable: %s", e)
    try:
        from datahub.secret.file_secret_store import FileSecretStore, FileSecretStoreConfig  # type: ignore

        if os.path.isdir(os.environ.get("DATAHUB_SECRET_DIR", "/mnt/secrets")):
            stores.append(FileSecretStore(FileSecretStoreConfig()))
    except Exception as e:  # noqa: BLE001
        logger.debug("workflow-actions: file secret store unavailable: %s", e)
    try:
        from datahub.secret.environment_secret_store import EnvironmentSecretStore, EnvironmentSecretStoreConfig  # type: ignore

        stores.append(EnvironmentSecretStore(EnvironmentSecretStoreConfig()))
    except Exception as e:  # noqa: BLE001
        logger.debug("workflow-actions: environment secret store unavailable: %s", e)
    return stores


def resolve_recipe_config(recipe: str, graph: Any) -> Tuple[str, Dict[str, Any]]:
    """Returns ``(source.type, resolved source.config)`` for an ingestion source recipe (JSON or YAML)."""
    try:
        data = json.loads(recipe)
    except json.JSONDecodeError:
        import yaml

        data = yaml.safe_load(recipe)
    if not isinstance(data, Mapping):
        raise ConnectionError_("ingestion source recipe is not an object")
    resolved: Any = None
    try:
        from datahub.secret.secret_common import resolve_recipe  # type: ignore

        resolved = resolve_recipe(json.dumps(data), _secret_stores(graph))
    except Exception as e:  # noqa: BLE001 — fall back to env-only substitution
        logger.warning("workflow-actions: secret resolution through DataHub failed (%s); falling back to environment variables", e)
    if not isinstance(resolved, Mapping):
        resolved = json.loads(substitute_env(json.dumps(data)))
    source = resolved.get("source") or {}
    source_type = str(source.get("type") or "")
    config = source.get("config") or {}
    if not source_type or not isinstance(config, Mapping):
        raise ConnectionError_("ingestion source recipe has no source.type / source.config")
    return source_type, dict(config)


class ConnectionResolver:
    """Turns declared connection specs into SQLAlchemy URLs, caching per (name, entity)."""

    def __init__(self, specs: Mapping[str, ConnectionSpec], graph: Any = None, env: Optional[Mapping[str, str]] = None):
        self.specs = dict(specs)
        self.graph = graph
        self.env = env
        self._cache: Dict[Tuple[str, str], ResolvedConnection] = {}

    @classmethod
    def from_config(cls, raw: Optional[Mapping[str, Any]], graph: Any = None, env: Optional[Mapping[str, str]] = None) -> "ConnectionResolver":
        return cls(parse_connections(raw), graph=graph, env=env)

    def names(self) -> List[str]:
        return list(self.specs)

    def spec(self, name: str) -> Optional[ConnectionSpec]:
        return self.specs.get(name)

    def describe(self, name: str, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        """What *would* be used — safe for dry runs (no secret lookups, no URLs)."""
        spec = self.specs.get(name)
        if spec is None:
            return {"connection": name, "kind": "undeclared"}
        out: Dict[str, Any] = {"connection": name, "kind": spec.kind}
        if spec.kind == "url":
            out["dialect"] = dialect_of(spec.url or "")
        elif spec.kind == "ingestionSource":
            out["ingestionSource"] = spec.ingestion_source
        elif spec.kind == "fromEntity":
            out["entity"] = _entity_urn(context)
        if spec.platform:
            out["platform"] = spec.platform
        return out

    def resolve(self, name: str, context: Optional[Mapping[str, Any]] = None) -> ResolvedConnection:
        spec = self.specs.get(name)
        if spec is None:
            raise ConnectionError_(f"unknown connection '{name}' — declare it under connections in the action config")
        entity = _entity_urn(context) if spec.kind == "fromEntity" else ""
        key = (name, entity or "")
        if key in self._cache:
            return self._cache[key]
        if spec.kind == "url":
            url = substitute_env(spec.url or "", self.env)
            resolved = ResolvedConnection(name=name, kind="url", url=url, dialect=dialect_of(url))
        elif spec.kind == "ingestionSource":
            resolved = self._from_source(name, spec.kind, self._fetch(INGESTION_SOURCE_QUERY, "ingestionSource", spec.ingestion_source or ""))
        else:
            if not entity:
                raise ConnectionError_(f"connection '{name}' resolves from the event entity, but this event has no entity urn")
            resolved = self._from_source(name, spec.kind, self._fetch(INGESTION_SOURCE_FOR_ENTITY_QUERY, "ingestionSourceForEntity", entity))
        self._cache[key] = resolved
        return resolved

    def _fetch(self, query: str, root: str, urn: str) -> Dict[str, Any]:
        if self.graph is None:
            raise ConnectionError_(f"resolving {root} needs a DataHub connection (graph); not available here")
        data = self.graph.execute_graphql(query, variables={"urn": urn}) or {}
        source = data.get(root) if isinstance(data, Mapping) else None
        if not isinstance(source, Mapping):
            raise ConnectionError_(f"{root}({urn}) returned nothing — no ingestion source found")
        return dict(source)

    def _from_source(self, name: str, kind: str, source: Mapping[str, Any]) -> ResolvedConnection:
        config = source.get("config") or {}
        if (config.get("executorId") or "") == CLI_EXECUTOR_ID:
            raise ConnectionError_(f"ingestion source {source.get('urn')} is CLI-managed; its credentials aren't available to the executor")
        recipe = config.get("recipe")
        if not recipe:
            raise ConnectionError_(f"ingestion source {source.get('urn')} has no recipe")
        source_type, source_config = resolve_recipe_config(str(recipe), self.graph)
        url, kwargs = url_from_recipe(source_type, source_config)
        return ResolvedConnection(
            name=name,
            kind=kind,
            url=url,
            dialect=dialect_of(url),
            engine_kwargs=kwargs,
            source_urn=str(source.get("urn") or ""),
            source_type=source_type,
        )


def _entity_urn(context: Optional[Mapping[str, Any]]) -> str:
    entity = (context or {}).get("entity") if isinstance(context, Mapping) else None
    urn = entity.get("urn") if isinstance(entity, Mapping) else None
    return str(urn or "")
