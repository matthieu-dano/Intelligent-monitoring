import json

from intelligent_pipeline.ops.process import generate_query


def test_generate_query_basic():
    metrics = ["metric1"]
    start_absolute = 1625151600000
    expected_query = {
        "metrics": [{"name": "metric1"}],
        "time_zone": "UCT",
        "start_absolute": start_absolute,
    }
    result = generate_query(metrics, start_absolute)
    assert json.loads(result) == expected_query


def test_generate_query_with_end_absolute():
    metrics = ["metric1"]
    start_absolute = 1625151600000
    end_absolute = 1625155200000
    expected_query = {
        "metrics": [{"name": "metric1"}],
        "time_zone": "UCT",
        "start_absolute": start_absolute,
        "end_absolute": end_absolute,
    }
    result = generate_query(metrics, start_absolute, end_absolute)
    assert json.loads(result) == expected_query


def test_generate_query_with_tags():
    metrics = ["metric1"]
    start_absolute = 1625151600000
    tag = {"host": "server1"}
    expected_query = {
        "metrics": [{"name": "metric1", "tags": {"host": ["server1"]}}],
        "time_zone": "UCT",
        "start_absolute": start_absolute,
    }
    result = generate_query(metrics, start_absolute, tag=tag)
    assert json.loads(result) == expected_query


def test_generate_query_with_resample():
    metrics = ["metric1"]
    start_absolute = 1625151600000
    resample = 60
    expected_query = {
        "metrics": [
            {
                "name": "metric1",
                "aggregators": [
                    {
                        "name": "first",
                        "align_end_time": False,
                        "align_start_time": True,
                        "sampling": {"value": "60", "unit": "SECONDS"},
                        "trim": False,
                    }
                ],
            }
        ],
        "time_zone": "UCT",
        "start_absolute": start_absolute,
    }
    result = generate_query(metrics, start_absolute, resample=resample)
    assert json.loads(result) == expected_query


def test_generate_query_with_all_parameters():
    metrics = ["metric1"]
    start_absolute = 1625151600000
    end_absolute = 1625155200000
    tag = {"host": "server1"}
    resample = 60
    expected_query = {
        "metrics": [
            {
                "name": "metric1",
                "tags": {"host": ["server1"]},
                "aggregators": [
                    {
                        "name": "first",
                        "align_end_time": False,
                        "align_start_time": True,
                        "sampling": {"value": "60", "unit": "SECONDS"},
                        "trim": False,
                    }
                ],
            }
        ],
        "time_zone": "UCT",
        "start_absolute": start_absolute,
        "end_absolute": end_absolute,
    }
    result = generate_query(metrics, start_absolute, end_absolute, tag, resample)
    assert json.loads(result) == expected_query
