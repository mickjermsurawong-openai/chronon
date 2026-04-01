#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.


import gen_thrift.common.ttypes as common
import pytest
from gen_thrift.api import ttypes

from ai.chronon import group_by, query


@pytest.fixture
def sum_op():
    return ttypes.Operation.SUM


@pytest.fixture
def min_op():
    return ttypes.Operation.MIN


@pytest.fixture
def days_unit():
    return common.TimeUnit.DAYS


@pytest.fixture
def hours_unit():
    return common.TimeUnit.HOURS


def event_source(table):
    """
    Sample left join
    """
    return ttypes.EventSource(
        table=table,
        query=ttypes.Query(
            startPartition="2020-04-09",
            selects={"subject": "subject_sql", "event_id": "event_sql", "cnt": 1},
            timeColumn="CAST(ts AS DOUBLE)",
        ),
    )


def entity_source(snapshotTable, mutationTable):
    """
    Sample source
    """
    return ttypes.EntitySource(
        snapshotTable=snapshotTable,
        mutationTable=mutationTable,
        query=ttypes.Query(
            startPartition="2020-04-09",
            selects={"subject": "subject_sql", "event_id": "event_sql", "cnt": 1},
            timeColumn="CAST(ts AS DOUBLE)",
            mutationTimeColumn="__mutationTs",
            reversalColumn="is_reverse",
        ),
    )


def test_pretty_window_str(days_unit, hours_unit):
    """
    Test pretty window utils.
    """
    window = common.Window(length=7, timeUnit=days_unit)
    assert group_by.window_to_str_pretty(window) == "7 days"
    window = common.Window(length=2, timeUnit=hours_unit)
    assert group_by.window_to_str_pretty(window) == "2 hours"


def test_pretty_operation_str(sum_op, min_op):
    """
    Test pretty operation util.
    """
    assert group_by.op_to_str(sum_op) == "sum"
    assert group_by.op_to_str(min_op) == "min"


def test_select():
    """
    Test select builder
    """
    assert query.selects("subject", event="event_expr") == {
        "subject": "subject",
        "event": "event_expr",
    }


def test_contains_windowed_aggregation(sum_op, min_op, days_unit):
    """
    Test checker for windowed aggregations
    """
    assert not group_by.contains_windowed_aggregation([])
    aggregations = [
        ttypes.Aggregation(inputColumn="event", operation=sum_op),
        ttypes.Aggregation(inputColumn="event", operation=min_op),
    ]
    assert not group_by.contains_windowed_aggregation(aggregations)
    aggregations.append(
        ttypes.Aggregation(
            inputColumn="event",
            operation=sum_op,
            windows=[common.Window(length=7, timeUnit=days_unit)],
        )
    )
    assert group_by.contains_windowed_aggregation(aggregations)


def test_validator_ok():
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(
                inputColumn="event_id", operation=ttypes.Operation.SUM
            ),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
            percentile=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.APPROX_PERCENTILE([0.5, 0.75]),
            ),
        ),
        version=0,
    )
    assert all(
        [
            agg.inputColumn
            for agg in gb.aggregations
            if agg.operation != ttypes.Operation.COUNT
        ]
    )
    group_by.validate_group_by(gb)
    with pytest.raises(ValueError):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                percentile=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.APPROX_PERCENTILE([1.5]),
                ),
            ),
            version=0
        )
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=None,
            version=0,
        )
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=entity_source("table", "mutationTable"),
            keys=["subject"],
            aggregations=None,
            version=0
        )
    group_by.GroupBy(
        sources=entity_source("table", None),
        keys=["subject"],
        aggregations=None,
        version=0,
    )


def test_generic_collector():
    aggregation = group_by.Aggregation(
        input_column="test", operation=group_by.Operation.APPROX_PERCENTILE([0.4, 0.2])
    )
    assert aggregation.argMap == {"k": "20", "percentiles": "[0.4, 0.2]"}


def test_select_sanitization():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(  # No selects are spcified
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            ),
            ttypes.EntitySource(  # Some selects are specified
                snapshotTable="entity_table1",
                query=query.Query(
                    selects={"key1": "key1_sql", "event_id": "event_sql"}
                ),
            ),
        ],
        keys=["key1", "key2"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(
                inputColumn="event_id", operation=ttypes.Operation.SUM
            ),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
        version=0,
    )
    required_selects = set(["key1", "key2", "event_id", "cnt"])
    assert set(gb.sources[0].events.query.selects.keys()) == required_selects
    assert set(gb.sources[0].events.query.selects.values()) == required_selects
    assert set(gb.sources[1].entities.query.selects.keys()) == required_selects
    assert set(gb.sources[1].entities.query.selects.values()) == set(
        ["key1_sql", "key2", "event_sql", "cnt"]
    )


def test_snapshot_with_hour_aggregation():
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=[
                ttypes.EntitySource(  # Some selects are specified
                    snapshotTable="entity_table1",
                    query=query.Query(
                        selects={"key1": "key1_sql", "event_id": "event_sql"},
                        time_column="ts",
                    ),
                )
            ],
            keys=["key1"],
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(
                    inputColumn="event_id",
                    operation=ttypes.Operation.SUM,
                    windows=[
                        common.Window(1, common.TimeUnit.HOURS),
                    ],
                ),
            ),
            accuracy=group_by.Accuracy.SNAPSHOT,
            version=0,
        )


def test_additional_metadata():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            )
        ],
        keys=["key1", "key2"],
        aggregations=[
            group_by.Aggregation(
                input_column="event_id", operation=ttypes.Operation.SUM
            )
        ],
        tags={"to_deprecate": "true"},
        version=0,
    )
    assert gb.metaData.tags["to_deprecate"]


def test_windows_as_strings():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            )
        ],
        keys=["key1", "key2"],
        aggregations=[
            group_by.Aggregation(
                input_column="event_id",
                operation=ttypes.Operation.SUM,
                windows=["1h", "30d"],
            )
        ],
        tags={"to_deprecate": "true"},
        version=0
    )

    windows = gb.aggregations[0].windows

    assert len(windows) == 2
    assert windows[0] == common.Window(1, common.TimeUnit.HOURS)
    assert windows[1] == common.Window(30, common.TimeUnit.DAYS)

    assert gb.metaData.tags["to_deprecate"]

def test_query_api_obj():
    selects_map = {
        "key1": "key1",
        "event_id": "event_id",
        "ts": "ts",
        "mutationTs": "mutationTs",
        "ds": "ds",
    }

    wheres = ["key1 = 1"]
    start_partition = "2020-04-09"
    end_partition = "2020-04-10"
    time_column = "ts"
    mutation_time_column = "mutationTs"
    partition_column = "ds"
    partition_format = "yyyy-MM-dd"
    sub_partitions_to_wait_for = ["hr=23:00"]

    query_obj = query.Query(
        selects=selects_map,
        wheres=wheres,
        start_partition=start_partition,
        end_partition=end_partition,
        time_column=time_column,
        mutation_time_column=mutation_time_column,
        partition_column=partition_column,
        partition_format=partition_format,
        sub_partitions_to_wait_for=sub_partitions_to_wait_for
    )

    assert query_obj.selects == selects_map
    assert query_obj.wheres == wheres
    assert query_obj.startPartition == start_partition
    assert query_obj.endPartition == end_partition
    assert query_obj.timeColumn == time_column
    assert query_obj.mutationTimeColumn == mutation_time_column
    assert query_obj.partitionColumn == partition_column
    assert query_obj.partitionFormat == partition_format
    assert query_obj.subPartitionsToWaitFor == sub_partitions_to_wait_for


def test_query_with_time_partitioned_flag():
    """
    Test Query with time_partitioned flag for tables using timestamp/date columns
    instead of traditional Hive-style string partitioning.
    """
    query_obj = query.Query(
        selects={"key1": "key1", "value": "value"},
        time_column="UNIX_TIMESTAMP(created_at) * 1000",
        partition_column="created_at",
        time_partitioned=True,
    )

    assert query_obj.timePartitioned == True
    assert query_obj.partitionColumn == "created_at"


def test_query_without_time_partitioned_flag():
    """
    Test Query without time_partitioned flag (default Hive-style partitioning).
    """
    query_obj = query.Query(
        selects={"key1": "key1"},
        partition_column="ds",
    )

    assert query_obj.partitionColumn == "ds"
    assert query_obj.timePartitioned is None  # Not set

def test_online_schedule_validation():
    """Test that online_schedule validation works correctly."""
    # Test that online_schedule cannot be set when online=False
    with pytest.raises(ValueError, match="online_schedule cannot be set when online=False"):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                count=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.COUNT
                ),
            ),
            version=1,
            online=False,
            online_schedule="@daily"  # This should raise an error
        )

    # Test that online_schedule can be None when online=False
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=False,
        online_schedule=None  # This should be fine
    )
    assert gb.metaData.executionInfo.onlineSchedule is None

    # Test that online_schedule defaults to @daily when online=True and not specified
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule=None  # Should default to @daily
    )
    assert gb.metaData.executionInfo.onlineSchedule == "@daily"

    # Test that online_schedule can be explicitly set when online=True
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule="0 2 * * *"  # Custom schedule
    )
    assert gb.metaData.executionInfo.onlineSchedule == "0 2 * * *"

    # Test that @never disables online scheduling even when online=True
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule="@never"
    )
    assert gb.metaData.executionInfo.onlineSchedule is None

    # Test that @never is accepted even when online=False
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=False,
        online_schedule="@never"
    )
    assert gb.metaData.executionInfo.onlineSchedule is None
