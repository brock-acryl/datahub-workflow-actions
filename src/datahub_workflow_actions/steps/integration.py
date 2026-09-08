"""Integration steps: webhook, Slack, Teams, email, Jira, wait.

Secrets come from the environment (or params, when a deployment prefers
that): ``SLACK_BOT_TOKEN``, ``SMTP_HOST/PORT/USER/PASSWORD/FROM``,
``JIRA_EMAIL`` / ``JIRA_API_TOKEN``."""

from __future__ import annotations

import json
import smtplib
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from datahub_workflow_actions.steps import RunContext, StepParams, step

JsonLike = Union[str, dict, list, None]


def _as_json(value: JsonLike, what: str) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(f"{what} is not valid JSON: {e}") from e


def _response_body(response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


class WebhookParams(StepParams):
    url: str
    method: str = Field("POST", description="POST, PUT, PATCH, GET, DELETE")
    headers: JsonLike = Field(None, description='JSON object, e.g. {"Authorization": "Bearer …"}')
    body: JsonLike = Field(None, description="JSON (templated) or a raw string.")
    expectStatus: Optional[List[int]] = Field(None, description="Acceptable status codes (default: any 2xx).")


@step(
    "webhook",
    label="Call webhook",
    description="HTTP request to an external system.",
    group="Integration",
    params=WebhookParams,
    outputs={"status": "HTTP status code", "body": "Parsed JSON body (or text)", "headers": "Response headers"},
)
def webhook(p: WebhookParams, ctx: RunContext) -> dict:
    # Header values must be strings: a templated `{{ n | length }}` renders as an int.
    headers = {str(k): str(v) for k, v in (_as_json(p.headers, "headers") or {}).items()}
    body = p.body
    kwargs: Dict[str, Any] = {"headers": headers, "timeout": ctx.timeout or 30}
    if body not in (None, ""):
        parsed = None
        if isinstance(body, (dict, list)):
            parsed = body
        else:
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = None
        if parsed is not None:
            kwargs["json"] = parsed
        else:
            kwargs["data"] = body
    if ctx.dry_run:
        return {"dryRun": True, "request": {"method": p.method.upper(), "url": p.url, **{k: v for k, v in kwargs.items() if k != "timeout"}}}
    response = ctx.session().request(p.method.upper(), p.url, **kwargs)
    acceptable = p.expectStatus or list(range(200, 300))
    if response.status_code not in acceptable:
        raise RuntimeError(f"webhook {p.method.upper()} {p.url} returned {response.status_code}: {response.text[:500]}")
    return {"status": response.status_code, "body": _response_body(response), "headers": dict(response.headers)}


class SlackParams(StepParams):
    channel: Optional[str] = Field(None, description="Channel id or #name (bot token mode).")
    text: str
    webhookUrl: Optional[str] = Field(None, description="Incoming-webhook URL; when set, channel/token are not used.")
    token: Optional[str] = Field(None, description="Bot token; defaults to $SLACK_BOT_TOKEN.")


@step("slack", label="Post to Slack", description="Send a message to a channel.", group="Integration", params=SlackParams, outputs={"ts": "Message timestamp", "channel": "Channel id"})
def slack(p: SlackParams, ctx: RunContext) -> dict:
    if p.webhookUrl:
        if ctx.dry_run:
            return {"dryRun": True, "webhookUrl": p.webhookUrl, "text": p.text}
        response = ctx.session().post(p.webhookUrl, json={"text": p.text}, timeout=ctx.timeout or 30)
        if response.status_code >= 300:
            raise RuntimeError(f"slack webhook returned {response.status_code}: {response.text[:300]}")
        return {"ok": True}
    token = p.token or ctx.env.get("SLACK_BOT_TOKEN")
    if not p.channel:
        raise ValueError("slack: channel is required unless webhookUrl is set")
    if ctx.dry_run:
        return {"dryRun": True, "channel": p.channel, "text": p.text, "hasToken": bool(token)}
    if not token:
        raise ValueError("slack: no token — set params.token or SLACK_BOT_TOKEN")
    response = ctx.session().post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}"},
        json={"channel": p.channel, "text": p.text},
        timeout=ctx.timeout or 30,
    )
    data = _response_body(response)
    if response.status_code >= 300 or not (isinstance(data, dict) and data.get("ok")):
        raise RuntimeError(f"slack chat.postMessage failed: {data}")
    return {"ts": data.get("ts"), "channel": data.get("channel")}


class TeamsParams(StepParams):
    webhookUrl: str
    text: str
    title: Optional[str] = None


@step("teams", label="Post to Microsoft Teams", description="Send a message to a Teams channel via incoming webhook.", group="Integration", params=TeamsParams)
def teams(p: TeamsParams, ctx: RunContext) -> dict:
    payload: Dict[str, Any] = {"text": p.text}
    if p.title:
        payload["title"] = p.title
    if ctx.dry_run:
        return {"dryRun": True, "webhookUrl": p.webhookUrl, "payload": payload}
    response = ctx.session().post(p.webhookUrl, json=payload, timeout=ctx.timeout or 30)
    if response.status_code >= 300:
        raise RuntimeError(f"teams webhook returned {response.status_code}: {response.text[:300]}")
    return {"status": response.status_code}


class EmailParams(StepParams):
    to: Union[str, List[str]] = Field(description="Recipient(s), comma-separated or list.")
    subject: str
    body: str
    sender: Optional[str] = Field(None, description="Defaults to $SMTP_FROM.")
    html: bool = False


@step("email", label="Send email", description="Send an email over SMTP (SMTP_HOST/PORT/USER/PASSWORD/FROM).", group="Integration", params=EmailParams)
def email(p: EmailParams, ctx: RunContext) -> dict:
    recipients = [r.strip() for r in (p.to.split(",") if isinstance(p.to, str) else p.to) if str(r).strip()]
    sender = p.sender or ctx.env.get("SMTP_FROM")
    if ctx.dry_run:
        return {"dryRun": True, "to": recipients, "subject": p.subject, "from": sender}
    host = ctx.env.get("SMTP_HOST")
    if not host or not sender:
        raise ValueError("email: SMTP_HOST and SMTP_FROM (or params.sender) are required")
    message = EmailMessage()
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message["Subject"] = p.subject
    message.set_content(p.body, subtype="html" if p.html else "plain")
    port = int(ctx.env.get("SMTP_PORT", "587"))
    with smtplib.SMTP(host, port, timeout=ctx.timeout or 30) as client:
        if ctx.env.get("SMTP_STARTTLS", "true").lower() != "false":
            client.starttls()
        if ctx.env.get("SMTP_USER"):
            client.login(ctx.env["SMTP_USER"], ctx.env.get("SMTP_PASSWORD", ""))
        client.send_message(message)
    return {"to": recipients}


class JiraParams(StepParams):
    baseUrl: str = Field(description="e.g. https://acme.atlassian.net")
    project: str = Field(description="Project key.")
    summary: str
    description: Optional[str] = None
    issueType: str = "Task"
    email: Optional[str] = Field(None, description="Defaults to $JIRA_EMAIL.")
    apiToken: Optional[str] = Field(None, description="Defaults to $JIRA_API_TOKEN.")


@step("jira_issue", label="Create Jira issue", description="Create an issue via the Jira Cloud REST API.", group="Integration", params=JiraParams, outputs={"key": "Issue key", "id": "Issue id", "url": "Browse URL"})
def jira_issue(p: JiraParams, ctx: RunContext) -> dict:
    payload = {
        "fields": {
            "project": {"key": p.project},
            "summary": p.summary,
            "issuetype": {"name": p.issueType},
            **({"description": p.description} if p.description else {}),
        }
    }
    if ctx.dry_run:
        return {"dryRun": True, "url": f"{p.baseUrl.rstrip('/')}/rest/api/2/issue", "payload": payload}
    user = p.email or ctx.env.get("JIRA_EMAIL")
    token = p.apiToken or ctx.env.get("JIRA_API_TOKEN")
    if not user or not token:
        raise ValueError("jira_issue: JIRA_EMAIL and JIRA_API_TOKEN (or params) are required")
    response = ctx.session().post(
        f"{p.baseUrl.rstrip('/')}/rest/api/2/issue", json=payload, auth=(user, token), timeout=ctx.timeout or 30
    )
    if response.status_code >= 300:
        raise RuntimeError(f"jira returned {response.status_code}: {response.text[:500]}")
    data = response.json()
    return {"key": data.get("key"), "id": data.get("id"), "url": f"{p.baseUrl.rstrip('/')}/browse/{data.get('key')}"}


class WaitParams(StepParams):
    seconds: float = Field(gt=0, le=3600)


@step("wait", label="Wait", description="Pause before the next step.", group="Integration", params=WaitParams)
def wait(p: WaitParams, ctx: RunContext) -> dict:
    if not ctx.dry_run:
        ctx.sleep(p.seconds)
    return {"waited": p.seconds}
