import json
from pathlib import Path

import pytest

from datahub_workflow_actions.templating import TemplateError, render_params, render_value

GOLDEN = json.loads((Path(__file__).resolve().parents[1] / "contracts" / "golden" / "templates.json").read_text())


@pytest.mark.parametrize("vector", GOLDEN["vectors"], ids=[v["template"] for v in GOLDEN["vectors"]])
def test_golden_vectors(vector):
    if vector.get("error"):
        with pytest.raises(TemplateError):
            render_value(vector["template"], GOLDEN["context"])
        return
    result = render_value(vector["template"], GOLDEN["context"])
    if "expected_object" in vector:
        assert result == vector["expected_object"]
    else:
        assert result == vector["expected"]


def test_render_params_recurses_and_keeps_types():
    params = {"url": "https://x/{{ requester.urn | urn_name }}", "body": {"owners": "{{ entity.owners }}", "n": 3}, "list": ["{{ form.field_abc }}", "lit"]}
    out = render_params(params, GOLDEN["context"])
    assert out == {"url": "https://x/jdoe", "body": {"owners": ["urn:li:corpuser:a", "urn:li:corpGroup:g"], "n": 3}, "list": ["restricted", "lit"]}


def test_sandbox_blocks_dangerous_access():
    with pytest.raises(TemplateError):
        render_value("{{ ''.__class__.__mro__ }}", GOLDEN["context"])
