"""Single-namespace schema for the filesystem POC."""

from __future__ import annotations


def fs_schema() -> dict[str, object]:
    return {
        "path": {"type": "string", "glob": True},
        "parent": "string",
        "basename": {"type": "string", "glob": True},
        "kind": "string",
        "ext": "string",
        "mime": "string",
        "size_bytes": "uint",
        "is_text": "uint",
        "text": {"type": "string", "glob": True},
        "blob_b64": {"type": "string", "filterable": False},
    }
