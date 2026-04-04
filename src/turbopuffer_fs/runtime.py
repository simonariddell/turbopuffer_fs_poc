"""Thin plan executor for turbopuffer plans."""

from __future__ import annotations

from collections.abc import Iterable

from .checks import run_check
from .paths import with_after_filter


def to_plain(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain(item) for item in value]
    if hasattr(value, "model_dump"):
        return to_plain(value.model_dump())
    if hasattr(value, "to_dict"):
        return to_plain(value.to_dict())
    if hasattr(value, "__dict__"):
        return {
            key: to_plain(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return value


def _rows_of(response) -> list[dict[str, object]]:
    if isinstance(response, dict):
        rows = response.get("rows", [])
    else:
        rows = getattr(response, "rows", [])
    return [to_plain(row) for row in (rows or [])]


def _normalized_query(name: str, response, *, pages: list[dict[str, object]] | None = None) -> dict[str, object]:
    plain = to_plain(response)
    if isinstance(plain, dict):
        billing = plain.get("billing")
        performance = plain.get("performance")
        aggregations = plain.get("aggregations")
        aggregation_groups = plain.get("aggregation_groups")
    else:
        billing = getattr(response, "billing", None)
        performance = getattr(response, "performance", None)
        aggregations = getattr(response, "aggregations", None)
        aggregation_groups = getattr(response, "aggregation_groups", None)
    result = {
        "name": name,
        "rows": _rows_of(response),
        "billing": to_plain(billing),
        "performance": to_plain(performance),
        "aggregations": to_plain(aggregations),
        "aggregation_groups": to_plain(aggregation_groups),
    }
    if pages is not None:
        result["pages"] = pages
        result["page_count"] = len(pages)
    return result


def _normalized_write(name: str, response) -> dict[str, object]:
    plain = to_plain(response)
    if isinstance(plain, dict):
        plain["name"] = name
        return plain
    return {
        "name": name,
        "response": plain,
    }


def _namespace_id(value) -> str:
    if isinstance(value, dict):
        return str(value["id"])
    identifier = getattr(value, "id", None)
    if identifier is None:
        raise ValueError(f"namespace value has no id: {value!r}")
    return str(identifier)


def _normalized_namespaces(name: str, response) -> dict[str, object]:
    if isinstance(response, dict):
        items = list(response.get("namespaces", []))
        next_cursor = response.get("next_cursor")
    else:
        items = list(response) if isinstance(response, Iterable) else list(getattr(response, "namespaces", []))
        next_cursor = getattr(response, "next_cursor", None)
    return {
        "name": name,
        "namespaces": [{"id": _namespace_id(item)} for item in items],
        "next_cursor": next_cursor,
    }


def paginate_ordered_query(namespace_handle, step: dict[str, object]) -> dict[str, object]:
    payload = dict(step["payload"])
    page_size = int(step.get("page_size", 256))
    limit = step.get("limit")
    order_field = str(step.get("order_field", "path"))
    last_value = None
    remaining = int(limit) if limit is not None else None
    rows: list[dict[str, object]] = []
    pages: list[dict[str, object]] = []

    while True:
        current_payload = dict(payload)
        current_payload["filters"] = with_after_filter(payload.get("filters"), order_field, last_value)
        current_payload["limit"] = page_size if remaining is None else min(page_size, remaining)
        response = namespace_handle.query(**current_payload)
        page = _normalized_query(step["name"], response)
        pages.append(page)
        page_rows = page["rows"]
        if not page_rows:
            break
        rows.extend(page_rows)
        if remaining is not None:
            remaining -= len(page_rows)
            if remaining <= 0:
                break
        if len(page_rows) < current_payload["limit"]:
            break
        last_value = str(page_rows[-1][order_field])

    return {
        "name": step["name"],
        "rows": rows,
        "pages": pages,
        "page_count": len(pages),
    }


def run_step(client, namespace_handle, step: dict[str, object], context: dict[str, object], results: dict[str, dict[str, object]]) -> dict[str, object]:
    kind = step["kind"]
    if kind == "query":
        if namespace_handle is None:
            raise ValueError("query step requires a namespace handle")
        if step.get("paginate"):
            return paginate_ordered_query(namespace_handle, step)
        response = namespace_handle.query(**step["payload"])
        return _normalized_query(step["name"], response)
    if kind == "write":
        if namespace_handle is None:
            raise ValueError("write step requires a namespace handle")
        response = namespace_handle.write(**step["payload"])
        return _normalized_write(step["name"], response)
    if kind == "namespaces":
        response = client.namespaces(**step.get("payload", {}))
        return _normalized_namespaces(step["name"], response)
    if kind == "assert":
        run_check(str(step["check"]), context, results)
        return {"name": step["name"], "status": "ok"}
    raise ValueError(f"unsupported plan step kind: {kind!r}")


def execute_plan(client, plan: dict[str, object]) -> dict[str, object]:
    context = dict(plan.get("context", {}))
    results: dict[str, dict[str, object]] = {}
    needs_namespace = any(step["kind"] in {"query", "write"} for step in plan.get("steps", []))
    namespace_handle = client.namespace(plan["namespace"]) if needs_namespace else None
    for step in plan.get("steps", []):
        result = run_step(client, namespace_handle, step, context, results)
        results[str(step["name"])] = result
    return {"plan": plan, "results": results}


def finalize_plan(plan: dict[str, object], executed: dict[str, object]):
    from .post import FINALIZERS

    finalizer = FINALIZERS[str(plan["finalize"])]
    return finalizer(dict(plan.get("context", {})), dict(executed.get("results", {})))


def run(client, plan: dict[str, object]):
    executed = execute_plan(client, plan)
    return finalize_plan(plan, executed)
