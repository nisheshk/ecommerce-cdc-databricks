import json
from datetime import datetime

import pytest

from ecommerce.generator.generate_events import (
    CDC_FEEDS,
    CDC_KEYS,
    SCENARIOS,
    EventGenerator,
    Knobs,
    generate,
    parse_duration,
    parse_pct,
    parse_start,
    scenario_knobs,
    write_batch,
)

START = parse_start("2026-01-01T00:00:00Z")


def run(params=None, batches=20, **kwargs):
    generator = EventGenerator(Knobs.from_dict(params or {}), START, **kwargs)
    return [lines for _, _, lines in generator.batches(batches)]


def lines_of(output, feed):
    return [line for batch in output for line in batch[feed]]


def records(output, feed):
    return [json.loads(line) for line in lines_of(output, feed)]


def epoch(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def test_same_seed_gives_identical_output():  # UT-07
    assert run({"seed": 42}) == run({"seed": 42})


def test_different_seed_gives_different_output():
    assert run({"seed": 42}) != run({"seed": 7})


def test_written_files_are_identical_across_runs(tmp_path):  # UT-07, on disk
    for folder in ("a", "b"):
        generator = EventGenerator(Knobs(), START)
        for index, batch_ts, lines in generator.batches(5):
            write_batch(str(tmp_path / folder), index, batch_ts, lines)
    files_a = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.json"))
    files_b = sorted(p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*.json"))
    assert files_a and files_a == files_b
    assert all((tmp_path / "a" / f).read_bytes() == (tmp_path / "b" / f).read_bytes() for f in files_a)
    assert not list(tmp_path.rglob(".*.tmp"))


def test_cdc_envelope_shape():
    output = run()
    for feed in CDC_FEEDS:
        for record in records(output, feed):
            assert set(record) == {"op", "ts_ms", "source", "before", "after"}
            assert record["source"]["table"] == feed
            assert record["op"] in {"c", "u", "d", "r"}
            if record["op"] in {"c", "r"}:
                assert record["before"] is None
            if record["op"] == "d":
                assert record["after"] is None


def test_initial_snapshot():  # CDC-01
    first = run(batches=1, initial_customers=25, initial_products=10)[0]
    snapshot = [json.loads(line) for line in first["customers"] if json.loads(line)["op"] == "r"]
    assert len(snapshot) == 25
    assert len({r["after"]["customer_id"] for r in snapshot}) == 25


def test_knobs_off_is_clean():
    output = run()
    for feed in CDC_FEEDS:
        lines = lines_of(output, feed)
        assert len(lines) == len(set(lines)), f"duplicates in {feed}"
        lsns = [json.loads(line)["source"]["lsn"] for line in lines]
        assert lsns == sorted(lsns), f"out of order lsn in {feed}"
    assert all(r["total"] >= 0 for r in (o["after"] for o in records(output, "orders")) if r)


@pytest.mark.parametrize("name", ["cdc_chaos", "integration_chaos_mix"])
def test_ts_ms_increases_per_key_in_lsn_order(name):  # SEQUENCE BY change_ts is safe
    knobs = scenario_knobs(name, {"rate_per_sec": 50})
    output = [lines for _, _, lines in EventGenerator(knobs, START).batches(60)]
    for feed in CDC_FEEDS:
        latest = {}
        parsed = []
        for line in set(lines_of(output, feed)):  # drop exact replays
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        for record in sorted(parsed, key=lambda r: r["source"]["lsn"]):
            row = record["after"] or record["before"]
            key = tuple(row.get(col) for col in CDC_KEYS[feed])
            if None in key or record["op"] == "x":
                continue
            assert record["ts_ms"] > latest.get(key, -1), f"{feed} {key}: ts_ms tie or reversal"
            latest[key] = record["ts_ms"]


def test_out_of_order_writes_lower_lsn_after_higher():  # CDC-02
    output = run({"out_of_order_pct": "50%"})
    lsns = [r["source"]["lsn"] for r in records(output, "customers")]
    assert any(later < earlier for earlier, later in zip(lsns, lsns[1:]))


def test_duplicates_are_exact_replays():  # CDC-03
    lines = lines_of(run({"dup_pct": "10%"}), "orders")
    assert len(lines) > len(set(lines))


def test_delete_before_insert():  # CDC-05
    output = run({"delete_before_insert": "on", "rate_per_sec": 50}, batches=60)
    first_op = {}
    for record in records(output, "customers"):
        row = record["after"] or record["before"]
        first_op.setdefault(row["customer_id"], (record["op"], record["source"]["lsn"]))
    deleted_first = [cid for cid, (op, _) in first_op.items() if op == "d"]
    assert deleted_first


def test_bad_records():  # CDC-08
    output = run({"bad_record_pct": "20%"})
    orders, malformed = [], 0
    for line in lines_of(output, "orders"):
        try:
            orders.append(json.loads(line))
        except json.JSONDecodeError:
            malformed += 1
    assert malformed > 0
    customers = records(output, "customers")
    assert any(r["after"] and r["after"]["customer_id"] is None for r in customers)
    assert any(r["op"] == "x" for r in customers)
    assert any(r["after"] and r["after"]["total"] < 0 for r in orders if r["op"] == "c")


def test_late_events_have_past_event_time():  # LA-03
    output = run({"late_pct": "100%", "late_by": ["2h"]})
    created = [r for r in records(output, "orders") if r["op"] == "c"]
    assert created
    for record in created:
        lag = record["ts_ms"] / 1000 - epoch(record["after"]["order_ts"])
        assert abs(lag - 7200) < 1


def test_clock_skew_future_dates_events():  # LA-05
    clicks = records(run({"clock_skew": "+90s"}), "clicks")
    assert any(epoch(c["event_ts"]) > START + 1 for c in clicks)


def test_orphans_never_arrive():  # JN-10
    output = run({"orphan_pct": "20%"})
    known = {(r["after"] or r["before"])["customer_id"] for r in records(output, "customers")}
    orders = [r["after"] for r in records(output, "orders") if r["op"] == "c"]
    assert any(o["customer_id"] not in known for o in orders)


def test_early_facts_arrive_after_delay():  # JN-02
    output = run({"early_fact_pct": "20%", "late_dim_delay": "30s"})
    created_at = {r["after"]["customer_id"]: r["ts_ms"] for r in records(output, "customers") if r["op"] == "c"}
    early = [r for r in records(output, "orders")
             if r["op"] == "c" and created_at.get(r["after"]["customer_id"], 0) > r["ts_ms"]]
    assert early


def test_schema_v2_adds_columns():  # SE-01
    output = run({"schema_version": "v2"})
    assert all("marketing_opt_in" in r["after"] for r in records(output, "customers") if r["op"] == "r")
    assert all("discount_code" in r["after"] for r in records(output, "orders") if r["op"] == "c")
    v1 = run()
    assert not any("discount_code" in r["after"] for r in records(v1, "orders") if r["op"] == "c")


def test_parsers():
    assert parse_duration("2h") == 7200
    assert parse_duration("+90s") == 90
    assert parse_duration("-90s") == -90
    assert parse_pct("5%") == pytest.approx(0.05)
    assert parse_pct(0.05) == 0.05
    with pytest.raises(ValueError):
        Knobs.from_dict({"not_a_knob": 1})
    with pytest.raises(ValueError):
        Knobs.from_dict({"schema_version": "v3"})


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_scenarios_parse_and_run(name):
    knobs = scenario_knobs(name)
    generator = EventGenerator(knobs, START)
    assert any(lines for _, _, batch in generator.batches(2) for lines in batch.values())
    assert parse_duration(SCENARIOS[name]["duration"]) > 0


def test_scenario_overrides_win():
    assert scenario_knobs("late_arrival", {"late_pct": "50%"}).late_pct == 0.5
    with pytest.raises(ValueError):
        scenario_knobs("no_such_scenario")


def test_generate_writes_files(tmp_path):
    totals = generate(str(tmp_path), scenario_knobs("integration_happy_path"), "50s")
    assert sum(totals.values()) == sum(1 for p in tmp_path.rglob("*.json") for _ in p.open())
