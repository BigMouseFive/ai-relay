"""严格 JSON 输出契约：不 repair、不提取片段，只接受通过 Schema 的完整回答。"""
from __future__ import annotations

import pytest

from app.response_contract import ResponseValidationError, provider_prompt, validate_result


FORMAT = {
    "type": "json_schema",
    "name": "listing-copy",
    "schema": {
        "type": "object",
        "required": ["title", "bullets"],
        "additionalProperties": False,
        "properties": {
            "title": {"type": "string", "minLength": 1},
            "bullets": {
                "type": "array", "minItems": 1,
                "items": {"type": "string", "minLength": 1},
            },
        },
    },
}


def test_valid_json_is_normalized_and_instruction_is_appended():
    result = validate_result('{ "bullets": ["one"], "title": "Name" }', FORMAT)
    assert result == '{"bullets":["one"],"title":"Name"}'
    prompt = provider_prompt("写文案", FORMAT)
    assert prompt.startswith("写文案")
    assert "系统输出契约" in prompt
    assert "JSON Schema" in prompt


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ("```json\n{\"title\":\"Name\",\"bullets\":[\"one\"]}\n```", "response_invalid_json"),
        ('{"title":"Name"}', "response_schema_mismatch"),
        ('{"title":"Name","bullets":[]}', "response_schema_mismatch"),
        ("", "response_empty"),
    ],
)
def test_invalid_output_is_rejected_without_repair(raw, code):
    with pytest.raises(ResponseValidationError) as caught:
        validate_result(raw, FORMAT)
    assert caught.value.code == code
