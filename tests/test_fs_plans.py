from turbopuffer_fs import (
    find_plan,
    grep_plan,
    ls_plan,
    mkdir_plan,
    put_bytes_plan,
    put_text_plan,
    read_bytes_plan,
    read_text_plan,
    rm_plan,
    stat_plan,
)


def test_stat_plan_shape():
    plan = stat_plan("documents__fs", "/notes/todo.txt")
    assert plan["namespace"] == "documents__fs"
    assert plan["finalize"] == "stat"
    assert plan["steps"][0]["kind"] == "query"
    assert plan["steps"][0]["payload"]["filters"] == ("path", "Eq", "/notes/todo.txt")


def test_ls_plan_has_target_and_paginated_children():
    plan = ls_plan("documents__fs", "/notes", limit=50)
    assert [step["name"] for step in plan["steps"]] == ["target", "children"]
    assert plan["steps"][1]["paginate"] is True
    assert plan["steps"][1]["limit"] == 50
    assert plan["steps"][1]["payload"]["filters"] == ("parent", "Eq", "/notes")


def test_find_plan_uses_subtree_filter_and_kind():
    plan = find_plan("documents__fs", "/notes", glob="*.md", kind="file", ignore_case=True, limit=10)
    filters = plan["steps"][1]["payload"]["filters"]
    assert plan["finalize"] == "find"
    assert ("kind", "Eq", "file") in filters[1]
    assert ("basename", "IGlob", "*.md") in filters[1]
    assert plan["steps"][1]["paginate"] is True
    assert plan["steps"][1]["limit"] == 10


def test_grep_plan_has_coarse_text_filter():
    plan = grep_plan("documents__fs", "/notes", "oauth", ignore_case=True, glob="*.md")
    filters = plan["steps"][1]["payload"]["filters"][1]
    assert ("kind", "Eq", "file") in filters
    assert ("is_text", "Eq", 1) in filters
    assert ("basename", "IGlob", "*.md") in filters
    assert ("text", "IGlob", "*oauth*") in filters


def test_read_plans_use_content_fields():
    assert read_text_plan("documents__fs", "/a.txt")["finalize"] == "read_text"
    assert read_bytes_plan("documents__fs", "/a.txt")["finalize"] == "read_bytes"


def test_mkdir_plan_reads_then_writes_directory_rows():
    plan = mkdir_plan("documents__fs", "/a/b")
    assert [step["kind"] for step in plan["steps"]] == ["query", "assert", "write"]
    assert plan["steps"][0]["payload"]["limit"] == 3
    rows = plan["steps"][2]["payload"]["upsert_rows"]
    assert [row["path"] for row in rows] == ["/", "/a", "/a/b"]


def test_put_text_plan_ensures_parents_and_target():
    plan = put_text_plan("documents__fs", "/a/b.txt", "hello")
    rows = plan["steps"][2]["payload"]["upsert_rows"]
    assert [row["path"] for row in rows] == ["/", "/a", "/a/b.txt"]
    assert rows[-1]["text"] == "hello"
    assert rows[-1]["is_text"] == 1


def test_put_bytes_plan_marks_binary_target():
    plan = put_bytes_plan("documents__fs", "/a/data.bin", b"\x00\x01")
    target = plan["steps"][2]["payload"]["upsert_rows"][-1]
    assert target["is_text"] == 0
    assert "blob_b64" in target


def test_rm_plan_non_recursive_probes_children_and_deletes_by_id():
    plan = rm_plan("documents__fs", "/notes", recursive=False)
    assert [step["name"] for step in plan["steps"]] == ["target", "child_probe", "validate", "write"]
    assert plan["steps"][-1]["payload"]["delete_rows_from"] == "target"


def test_rm_plan_recursive_uses_delete_by_filter():
    plan = rm_plan("documents__fs", "/notes", recursive=True)
    assert plan["steps"][1]["name"] == "delete_targets"
    assert plan["steps"][-1]["payload"]["delete_rows_from"] == "delete_targets"
