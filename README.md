# datahub-workflow-actions

A generic [`datahub-actions`](https://github.com/acryldata/datahub-actions) action that runs **declarative rules** when
requests through a DataHub **Action Workflow** are created, decided, or completed. The rules are authored visually in
the Workflow Builder MFE and stored in an ingestion recipe; this package validates and executes them.

```
rule:  on (lifecycle event) → when (conditions) → steps (in order)
```

## What a rule can do

| Level | Options |
| --- | --- |
| Trigger | `CREATE`, `PENDING`, `MODIFY` (step decided, optional `stepId` + `result`), `COMPLETED` (+ `ACCEPTED` / `REJECTED` / `CANCELLED`) |
| Conditions | nested `AND`/`OR` groups over dotted context paths; `EQUAL`, `CONTAIN`, `START_WITH`, `END_WITH`, `EXISTS`, `IN`, `GREATER_THAN`, `LESS_THAN` (numbers and dates coerced), `MATCHES` (regex); `negated`, `caseInsensitive` |
| Steps | ordered; per-step `when`; `onError` (`fail` / `continue` / `stop`); `retry` (attempts, fixed or exponential backoff); `timeoutSeconds`; `forEach` fan-out (`item`, `index`); outputs exposed to later steps as `steps.<id>.output`; `idempotencyKey`; `enabled`; `description` |
| Step catalog | `add_tag`, `remove_tag`, `add_term`, `remove_term`, `add_owner`, `remove_owner`, `set_domain`, `clear_domain`, `add_to_data_product`, `set_structured_property`, `deprecate`, `undeprecate`, `update_description`, `webhook`, `slack`, `teams`, `email`, `jira_issue`, `wait` |

Every string parameter is a Jinja2 template over the **context document** (sandboxed, strict — a typo fails loudly):

```
event.{operation,result,stepId,time,actor,id}   request.{urn,description,status,result,resultNote,createdAt}
workflow.{urn,id,name,steps[],fields[]}          entity.{urn,type,name,platform,description,tags[],terms[],owners[],domain}
form.<fieldId>  form_by_name.<Field name>        requester.{urn,username,email,name,groups[]}   approver.{…}
decisions[]  params (raw event parameters)       steps.<id>.output   item / index (inside forEach)
```
Filters: `urn_name` (`urn:li:dataset:(…,db.sales.orders,PROD)` → `db.sales.orders`), `date`, `json`, plus Jinja built-ins.

## Install & run

```bash
pip install datahub-workflow-actions          # also registers the `datahub-workflow-actions` ingestion source
```

**From an ingestion recipe (what the MFE writes)** — see `examples/recipe.yaml`. The source starts a datahub-actions pipeline
in-process (Kafka `EntityChangeEvent_v1` → `workflow_actions`) and runs until stopped. Set the executor for the
"Workflow Actions" ingestion source and add this package as an extra pip requirement.

**As a plain datahub-actions pipeline:**

```yaml
name: workflow-actions
source: { type: kafka, config: { connection: { bootstrap: ${KAFKA_BOOTSTRAP_SERVER} } } }
filter: { event_type: EntityChangeEvent_v1, event: { entityType: actionRequest, category: LIFECYCLE } }
action:
  type: workflow_actions
  config: { rulesFile: /etc/datahub/workflow-rules.yaml, statePath: /var/lib/datahub/workflow-actions.db }
```

Secrets are read from the environment: `SLACK_BOT_TOKEN`, `SMTP_HOST/PORT/USER/PASSWORD/FROM`, `JIRA_EMAIL`, `JIRA_API_TOKEN`.
Idempotency state lives in a sqlite file (`statePath`, default `~/.datahub/workflow-actions-state.db`), so a redelivered
event never runs a step twice.

## CLI

```bash
workflow-actions schema                                  # JSON Schema the MFE vendors
workflow-actions catalog                                 # step catalog (params + outputs) for the MFE picker
workflow-actions validate recipe.yaml                    # contract + step types
workflow-actions simulate --rules recipe.yaml --event event.json [--fixtures fixtures.json] [--show-context]
workflow-actions simulate ... --execute --gms http://localhost:8080 --token $TOKEN   # really run it
```

`simulate` is a dry run: it resolves the context (from fixtures, or empty), evaluates every rule, renders every step's
parameters, and prints the plan — including which mutation each metadata step *would* send.

## Contract

`contract.py` is the source of truth (schemaVersion 1). `contracts/golden/templates.json` holds template vectors the MFE's
preview must reproduce byte-for-byte. Adding a step type = one `@step(...)` function with a params model; the catalog,
schema, CLI and MFE form follow.

## Development

```bash
pip install -e '.[dev]' && pytest
```

## SQL steps

`sql` runs templated statements on a **named connection** declared once in the action config — never in a rule. A
connection is one of three kinds, mirroring how DataHub Cloud assertions get their credentials:

```yaml
source:
  config:
    connections:
      warehouse:                                    # 1. reuse an ingestion source's recipe + secrets
        ingestionSource: urn:li:dataHubIngestionSource:abc
      pg_reports:                                   # 2. a SQLAlchemy URL; the password is a ${SECRET} reference
        url: postgresql://reports:${PG_REPORTS_PASSWORD}@host:5432/reports
        platform: postgres
      entity:                                       # 3. whichever source produced the event's entity
        fromEntity: true
    rules:
      - steps:
          - id: grant
            type: sql
            params:
              connection: warehouse
              statements:
                - GRANT SELECT ON TABLE {{ entity.urn | sql_table }} TO ROLE {{ form.field_role | sql_ident }}
              parameters: {}                  # :name bindings for values that can be bound
```

For kinds 1 and 3 the action fetches the source through GraphQL (`ingestionSource` / `ingestionSourceForEntity`, the
latter DataHub Cloud), resolves `${SECRETS}` in its recipe through the DataHub secret stores (UI secrets → mounted files
→ environment, same precedence as the executor) and builds a SQLAlchemy URL from the platform config — Snowflake,
Databricks/Unity Catalog (needs a `warehouse_id`), BigQuery (service-account `credential`), Postgres, Redshift, MySQL and
the other `host_port`/`sqlalchemy_uri` sources. CLI-managed sources are refused. Dry runs describe the connection
(kind, source, dialect) without fetching recipes or secrets. `validate` rejects literal passwords in URLs and `sql`
steps that name an undeclared connection. Extra keys in a spec (`platform`, `description`) are preserved for the MFE.

Identifiers cannot be bound, so statements are templates — and every `{{ }}` in a statement **must** pass through a
`sql_*` filter (`sql_ident`, `sql_literal`, `sql_table`, `sql_schema`, `sql_database`; dataset URNs decompose into
quoted `db.schema.table` for the connection's dialect). Unfiltered expressions fail validation and are refused at run time
unless the step sets `unsafeRawTemplates: true`. Install drivers as extra pip requirements (`snowflake-sqlalchemy`,
`psycopg2-binary`, `sqlalchemy-bigquery`, `databricks-sql-connector`).
