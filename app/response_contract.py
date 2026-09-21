"""模型输出契约：构造 JSON 约束提示词并严格校验最终回答。

此模块刻意不做 JSON repair、Markdown fence 剥离或片段提取。调用方声明
``json_schema`` 后，只有能被 ``json.loads`` 完整解析且通过 JSON Schema 校验的
文本才能成为任务成功结果。
"""
from __future__ import annotations

import json
from typing import Any, Mapping

from jsonschema import Draft202012Validator, SchemaError


class ResponseValidationError(ValueError):
    """模型已给出确定回答，但回答不满足调用方声明的输出契约。"""

    def __init__(self, message: str, code: str) -> None:
        self.code = code
        super().__init__(message)


def validate_response_format(response_format: Mapping[str, Any] | None) -> None:
    """在接收任务时校验 response_format，尽早拒绝无效 Schema。"""
    if response_format is None:
        return
    if response_format.get("type") != "json_schema":
        raise ValueError("仅支持 response_format.type=json_schema")
    schema = response_format.get("schema")
    if not isinstance(schema, dict):
        raise ValueError("response_format.schema 必须是 JSON 对象")
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as error:
        raise ValueError(f"response_format.schema 非法: {error.message}") from error


def response_instruction(response_format: Mapping[str, Any] | None) -> str:
    """返回附加给实际 provider prompt 的输出约束。"""
    if response_format is None:
        return ""
    validate_response_format(response_format)
    schema = json.dumps(response_format["schema"], ensure_ascii=False, separators=(",", ":"))
    return (
        "\n\n【系统输出契约（必须严格遵守）】\n"
        "你的最终回答必须且只能是一个完整、合法的 JSON 值。不要输出 Markdown 代码块、"
        "解释、前后缀、注释或思考过程。输出将用严格 JSON 解析与 JSON Schema 校验；"
        "任一不符合项都会判定本次任务失败。\n"
        f"JSON Schema：{schema}"
    )


def provider_prompt(prompt: str, response_format: Mapping[str, Any] | None) -> str:
    """构建要发送给浏览器/API/ACP 的最终 prompt。"""
    return prompt + response_instruction(response_format)


def validate_result(raw_text: str, response_format: Mapping[str, Any] | None) -> str:
    """严格验证回答；成功时返回规范化 JSON 文本，未声明契约时原样返回。"""
    if response_format is None:
        return raw_text
    validate_response_format(response_format)
    if not raw_text or not raw_text.strip():
        raise ResponseValidationError("模型返回空内容", "response_empty")
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as error:
        raise ResponseValidationError(
            f"模型返回不是合法 JSON: {error.msg}（第 {error.lineno} 行第 {error.colno} 列）",
            "response_invalid_json",
        ) from error

    validator = Draft202012Validator(response_format["schema"])
    errors = sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path))
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.absolute_path) or "$"
        raise ResponseValidationError(
            f"模型 JSON 不符合 Schema（{location}）: {first.message}",
            "response_schema_mismatch",
        )
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
