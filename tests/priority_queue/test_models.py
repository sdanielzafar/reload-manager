import pytest

from reloadmanager.priority_queue.models import QueueRecord, TableAttrRecord


def test_queue_record_to_sql_values():
    record = QueueRecord(
        source_table="schema.source",
        target_table="schema.target",
        event_time="2025-04-27 00:00:00",
        trigger_time=None,
        strategy="WriteNOS",
        lock_rows=True,
        status="Q",
        priority=5,
        event_time_latest=None
    )
    sql_values = record.to_sql_values()
    expected = (
        "('schema.source', 'schema.target', '2025-04-27 00:00:00', NULL, 'WriteNOS', true, 'Q', 5, NULL)"
    )
    assert sql_values == expected


def test_queue_record_fields_str():
    fields_str = QueueRecord.fields_str()
    expected_fields = "source_table, target_table, event_time, trigger_time, strategy, lock_rows, status, priority, event_time_latest"
    assert fields_str == expected_fields


def test_queue_record_getitem_and_len():
    record = QueueRecord(
        source_table="schema.source",
        target_table="schema.target",
        event_time="event",
        trigger_time="trigger",
        strategy="WriteNOS",
        lock_rows=True,
        status="Q",
        priority=5,
        event_time_latest="latest"
    )
    assert len(record) == 9
    assert record[0] == "schema.source"
    assert record[5] is True
    assert record[8] == "latest"


def test_queue_record_iter():
    record = QueueRecord(
        source_table="a",
        target_table="b",
        event_time="c",
        trigger_time="d",
        strategy="e",
        lock_rows=False,
        status="f",
        priority=1,
        event_time_latest="g"
    )
    iterated = list(record)
    expected = ["a", "b", "c", "d", "e", False, "f", 1, "g"]
    assert iterated == expected


def test_table_attr_record_from_tuple_success():
    rec = TableAttrRecord.from_tuple(
        tuple(["schema.table1", "schema.table2", "TPT", "false", 1, 0, 100])
    )
    assert rec.source_table == "schema.table1"
    assert rec.target_table == "schema.table2"
    assert rec.strategy == "TPT"
    assert rec.disabled is False
    assert rec.priority == 1
    assert rec.min_staleness == 0
    assert rec.max_staleness == 100


def test_table_attr_record_from_tuple_success2():
    rec = TableAttrRecord.from_tuple(
        tuple(["schema.table1", "schema.table2", "TPT", False, 1, 0, 100])
    )
    assert rec.source_table == "schema.table1"
    assert rec.target_table == "schema.table2"
    assert rec.strategy == "TPT"
    assert rec.disabled is False
    assert rec.priority == 1
    assert rec.min_staleness == 0
    assert rec.max_staleness == 100


def test_table_attr_record_from_tuple_missing_target_table():
    rec = TableAttrRecord.from_tuple(
        tuple(["schema.table1", "", "JDBC", "true", 5, 1, 10])
    )
    assert rec.source_table == "schema.table1"
    assert rec.target_table == "schema.table1"  # fallback


def test_table_attr_record_from_tuple_invalid_table_format():
    with pytest.raises(ValueError, match=r"must have 2 namespaces"):
        TableAttrRecord.from_tuple(
            tuple(["badtable", "schema.target", "TPT", "false", 1, 0, 100])
        )


def test_table_attr_record_from_tuple_invalid_strategy():
    with pytest.raises(ValueError, match=r"invalid method"):
        TableAttrRecord.from_tuple(
            tuple(["schema.source", "schema.target", "BadMethod", "false", 1, 0, 100])
        )


def test_table_attr_record_from_tuple_invalid_disabled_flag():
    with pytest.raises(ValueError, match=r"invalid disabled status"):
        TableAttrRecord.from_tuple(
            tuple(["schema.source", "schema.target", "TPT", "notabool", 1, 0, 100])
        )


def test_table_attr_record_from_tuple_wrong_length():
    with pytest.raises(ValueError, match=r"should have 7 fields"):
        TableAttrRecord.from_tuple(
            tuple(["only", "five", "fields", "false", 1])
        )


def test_queue_record_sql_escapes_single_quotes():
    record = QueueRecord(
        source_table="schema.source",
        target_table="schema.target",
        event_time="2025-04-27 00:00:00",
        trigger_time=None,
        strategy="Write'NOS",
        lock_rows=False,
        status="Q",
        priority=7,
        event_time_latest=None
    )
    sql_values = record.to_sql_values()
    assert "'WriteNOS'" in sql_values  # double quotes inside
