"""Thin query-plan executor over the turbopuffer Python client."""

from __future__ import annotations

from functools import partial, reduce

from .post import FINALIZERS


pipe = lambda value, *funcs: reduce(lambda acc, fn: fn(acc), funcs, value)


def to_dict(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "__dict__"):
        return {k: v for k, v in vars(value).items() if not k.startswith("_")}
    return value


def rows_of(response) -> list[dict[str, object]]:
    rows = response.get("rows") if isinstance(response, dict) else getattr(response, "rows", [])
    return list(map(to_dict, rows or []))


def normalize_response(name: str, response) -> dict[str, object]:
    return {
        "name": name,
        "rows": rows_of(response),
        "billing": to_dict(response.get("billing") if isinstance(response, dict) else getattr(response, "billing", None)),
        "performance": to_dict(response.get("performance") if isinstance(response, dict) else getattr(response, "performance", None)),
    }


def execute_step(namespace, step: dict[str, object]) -> dict[str, object]:
    response = namespace.query(**step["payload"])
    return normalize_response(step.get("name", "query"), response)


def execute_queries(client, plan: dict[str, object]) -> list[dict[str, object]]:
    namespace = client.namespace(plan["namespace"])
    return list(map(partial(execute_step, namespace), plan.get("queries", [])))


def finalize(plan: dict[str, object], query_results: list[dict[str, object]]):
    finalizer = FINALIZERS[plan["finalize"]]
    return finalizer(plan.get("context", {}), query_results)


def run(client, plan: dict[str, object]):
    return pipe(plan, partial(execute_queries, client), partial(finalize, plan))
