"""Seeded generator for e-commerce CDC events and clickstream, with chaos knobs.

Writes JSON Lines files in the landing layout from SPEC.md: four CDC feeds in a
Debezium-style envelope plus one append-only click stream.

    <output_root>/cdc/customers/    <output_root>/cdc/products/
    <output_root>/cdc/inventory/    <output_root>/cdc/orders/
    <output_root>/events/clicks/

Time is simulated, so the same seed, start time and knobs always produce the
same files (UT-07). Add --realtime to sleep between batches so Auto Loader sees
a steady trickle.

Example:
    python generate_events.py --output-root /Volumes/ecom_lab/landing/raw \\
        --duration 10m --realtime --params '{"late_pct": "5%", "dup_pct": "2%"}'
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Iterator

FEED_DIRS = {
    "customers": "cdc/customers",
    "products": "cdc/products",
    "inventory": "cdc/inventory",
    "orders": "cdc/orders",
    "clicks": "events/clicks",
}
CDC_FEEDS = ("customers", "products", "inventory", "orders")

# Mix of random actions. Order lifecycle, stock drops and restocks are
# scheduled on top of these.
ACTION_WEIGHTS = {
    "click": 55,
    "order": 20,
    "new_customer": 5,
    "customer_change": 12,
    "product_change": 8,
}
DELETE_BEFORE_INSERT_RATE = 0.05  # share of new customers affected when the knob is on

# Ready-made knob sets for the scenarios in SPEC.md.
SCENARIOS = {
    "baseline": {
        "covers": "LA-01, JN-01, CDC-01, CDC-06, UT-07",
        "duration": "5m",
        "knobs": {},
    },
    "late_arrival": {
        "covers": "LA-02, LA-03, LA-04, LA-08",
        "duration": "10m",
        "knobs": {"late_pct": "5%", "late_by": ["2m", "15m", "2h"]},
    },
    "future_dated": {
        "covers": "LA-05",
        "duration": "5m",
        "knobs": {"clock_skew": "+90s"},
    },
    "early_and_orphan_facts": {
        "covers": "JN-02, JN-03, JN-04, JN-05, JN-10",
        "duration": "15m",
        "knobs": {"early_fact_pct": "3%", "late_dim_delay": "10m", "orphan_pct": "1%"},
    },
    "cdc_chaos": {
        "covers": "CDC-02, CDC-03, CDC-04, CDC-05",
        "duration": "10m",
        "knobs": {"out_of_order_pct": "5%", "dup_pct": "2%", "delete_before_insert": "on"},
    },
    "bad_records": {
        "covers": "CDC-08, IT-02, IT-03",
        "duration": "5m",
        "knobs": {"bad_record_pct": "5%"},
    },
    "schema_v2": {
        "covers": "SE-01, SE-02, SE-06, IT-08",
        "duration": "5m",
        "knobs": {"schema_version": "v2"},
    },
    "integration_happy_path": {
        "covers": "IT-01, IT-04, IT-06",
        "duration": "50s",  # 20 events/s x 50s = 1,000 events
        "knobs": {"seed": 42},
    },
    "integration_chaos_mix": {
        "covers": "IT-05",
        "duration": "10m",
        "knobs": {"seed": 42, "late_pct": "5%", "dup_pct": "2%", "out_of_order_pct": "5%"},
    },
}
LOW_STOCK = 10
DEFAULT_START = "2026-01-01T00:00:00Z"

WAREHOUSES = ["WH-ON-1", "WH-BC-1", "WH-AB-1"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CLICK_FUNNEL = ["view", "add_to_cart", "checkout"]
DEVICES = ["mobile", "desktop", "tablet"]
DISCOUNT_CODES = ["SAVE10", "WELCOME15", "FALL20"]  # the number is the percent off
CITIES = [  # (city, province, area code)
    ("Toronto", "ON", "416"), ("Ottawa", "ON", "613"), ("London", "ON", "519"),
    ("Vancouver", "BC", "604"), ("Victoria", "BC", "250"),
    ("Calgary", "AB", "403"), ("Edmonton", "AB", "780"),
    ("Montreal", "QC", "514"), ("Quebec City", "QC", "418"),
    ("Halifax", "NS", "902"), ("Winnipeg", "MB", "204"), ("Saskatoon", "SK", "306"),
]
FIRST_NAMES = ["Ava", "Liam", "Noah", "Emma", "Olivia", "Ethan", "Priya", "Arjun",
               "Mei", "Lucas", "Sofia", "Omar", "Chloe", "Mateo", "Zara", "Yuki"]
LAST_NAMES = ["Smith", "Tremblay", "Patel", "Nguyen", "Brown", "Singh", "Martin",
              "Roy", "Wilson", "Chen", "Gagnon", "Khan", "Taylor", "Lee"]
STREETS = ["Main St", "King St", "Queen St", "Maple Ave", "Oak St", "Elm St",
           "Park Rd", "Lakeshore Blvd"]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.ca", "icloud.com"]
CATEGORIES = {
    "electronics": ["Headphones", "Charger", "Speaker", "Keyboard"],
    "home": ["Lamp", "Kettle", "Blanket", "Mug"],
    "apparel": ["Hoodie", "Toque", "Jacket", "Socks"],
    "outdoor": ["Tent", "Backpack", "Water Bottle", "Headlamp"],
}
ADJECTIVES = ["Classic", "Pro", "Eco", "Ultra", "Mini", "Smart"]

_DURATION = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?$")
_UNIT_SECONDS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value) -> float:
    """Seconds from 90, "90s", "+90s", "-90s", "15m" or "2h"."""
    if isinstance(value, (int, float)):
        return float(value)
    match = _DURATION.match(str(value).strip())
    if not match:
        raise ValueError(f"bad duration: {value!r}")
    return float(match.group(1)) * _UNIT_SECONDS[match.group(2) or "s"]


def parse_pct(value) -> float:
    """A fraction from "5%" or 0.05."""
    if isinstance(value, str):
        text = value.strip()
        value = float(text[:-1]) / 100 if text.endswith("%") else float(text)
    if not 0 <= value <= 1:
        raise ValueError(f"percentage out of range: {value!r}")
    return float(value)


def parse_switch(value) -> bool:
    """A bool from true/false or "on"/"off"."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text not in ("on", "off", "true", "false"):
        raise ValueError(f"expected on/off: {value!r}")
    return text in ("on", "true")


def parse_start(value: str) -> float:
    """Epoch seconds from an ISO-8601 time; a time without a zone is UTC."""
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def iso(ts: float) -> str:
    return (datetime.fromtimestamp(ts, tz=timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


@dataclass(frozen=True)
class Knobs:
    """One parameter dict per run. Every chaos knob defaults to off."""

    rate_per_sec: float = 20
    late_pct: float = 0.0
    late_by: tuple[float, ...] = (120.0, 900.0, 7200.0)
    early_fact_pct: float = 0.0
    late_dim_delay: float = 600.0
    orphan_pct: float = 0.0
    out_of_order_pct: float = 0.0
    dup_pct: float = 0.0
    delete_before_insert: bool = False
    bad_record_pct: float = 0.0
    schema_version: str = "v1"
    clock_skew: float = 0.0
    seed: int = 42

    @classmethod
    def from_dict(cls, params: dict) -> Knobs:
        unknown = set(params) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown knobs: {sorted(unknown)}")
        values = dict(params)
        for name in ("late_pct", "early_fact_pct", "orphan_pct",
                     "out_of_order_pct", "dup_pct", "bad_record_pct"):
            if name in values:
                values[name] = parse_pct(values[name])
        for name in ("late_dim_delay", "clock_skew"):
            if name in values:
                values[name] = parse_duration(values[name])
        if "late_by" in values:
            late_by = values["late_by"]
            late_by = late_by if isinstance(late_by, (list, tuple)) else [late_by]
            values["late_by"] = tuple(parse_duration(v) for v in late_by)
        if "delete_before_insert" in values:
            values["delete_before_insert"] = parse_switch(values["delete_before_insert"])
        if values.get("schema_version", "v1") not in ("v1", "v2"):
            raise ValueError(f"schema_version must be v1 or v2: {values['schema_version']!r}")
        if "rate_per_sec" in values:
            values["rate_per_sec"] = float(values["rate_per_sec"])
        if "seed" in values:
            values["seed"] = int(values["seed"])
        return cls(**values)


class EventGenerator:
    """Simulates the shop and returns one batch of JSON lines per feed at a time."""

    def __init__(self, knobs: Knobs, start_ts: float, batch_seconds: float = 5.0,
                 initial_customers: int = 100, initial_products: int = 40):
        self.k = knobs
        self.rng = random.Random(knobs.seed)
        self.now = start_ts
        self.batch_seconds = batch_seconds
        self.batch_index = 0
        self._initial = (initial_customers, initial_products)
        self._lsn = 0
        self._seq = 0  # tie-breaker so the schedule heap never compares callbacks
        self._schedule: list = []  # (due_ts, seq, callback, args)
        self._carry = 0.0
        self._next_id = {"customer": 1, "order": 1, "product": 1, "orphan": 900_000}
        self.customers: dict[int, dict] = {}
        self.products: dict[int, dict] = {}
        self.inventory: dict[tuple[int, str], dict] = {}
        self.orders: dict[int, dict] = {}  # open orders, until their final status
        self.sessions: dict[str, dict] = {}
        self._restocking: set[tuple[int, str]] = set()
        self._out: dict[str, list[str]] = {}
        self._next_out: dict[str, list[str]] = {feed: [] for feed in FEED_DIRS}

    def batches(self, count: int) -> Iterator[tuple[int, float, dict[str, list[str]]]]:
        for _ in range(count):
            yield self.next_batch()

    def next_batch(self) -> tuple[int, float, dict[str, list[str]]]:
        start, end = self.now, self.now + self.batch_seconds
        # Lines held back last batch (out of order, replays) lead this one.
        self._out, self._next_out = self._next_out, {feed: [] for feed in FEED_DIRS}
        if self.batch_index == 0:
            self._snapshot(start)
        wanted = self.k.rate_per_sec * self.batch_seconds + self._carry
        count = int(wanted)
        self._carry = wanted - count
        for ts in sorted(self.rng.uniform(start, end) for _ in range(count)):
            self._run_due(ts)
            self._random_action(ts)
        self._run_due(end)
        index = self.batch_index
        self.batch_index += 1
        self.now = end
        return index, start, self._out

    # -- plumbing ---------------------------------------------------------

    def _emit(self, feed: str, record, *, hold: bool = False) -> None:
        line = record if isinstance(record, str) else json.dumps(record, separators=(",", ":"))
        (self._next_out if hold else self._out)[feed].append(line)
        if self.rng.random() < self.k.dup_pct:
            self._next_out[feed].append(line)  # exact replay, like a Debezium failover

    def _cdc(self, table: str, op: str, before, after, ts: float) -> dict:
        self._lsn += 1
        return {"op": op, "ts_ms": int(ts * 1000),
                "source": {"table": table, "lsn": self._lsn},
                "before": before, "after": after}

    def _update(self, table: str, row: dict, changes: dict, ts: float, follow_up=None) -> None:
        """Emit a u for row. With out_of_order_pct, a second update is written
        first and this lower-lsn one is held back to the next batch."""
        before = dict(row)
        row.update(changes)
        record = self._cdc(table, "u", before, dict(row), ts)
        if follow_up and self.rng.random() < self.k.out_of_order_pct:
            before = dict(row)
            row.update(follow_up())
            self._emit(table, self._cdc(table, "u", before, dict(row), ts))
            self._emit(table, record, hold=True)
        else:
            self._emit(table, record)

    def _at(self, due: float, callback, *args) -> None:
        self._seq += 1
        heapq.heappush(self._schedule, (due, self._seq, callback, args))

    def _run_due(self, ts: float) -> None:
        while self._schedule and self._schedule[0][0] <= ts:
            due, _, callback, args = heapq.heappop(self._schedule)
            callback(due, *args)

    def _take_id(self, kind: str) -> int:
        value = self._next_id[kind]
        self._next_id[kind] += 1
        return value

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _event_time(self, ts: float) -> float:
        """Event time for a fact: pushed into the past by late_pct, ahead by clock_skew."""
        if self.rng.random() < self.k.late_pct:
            ts -= self.rng.choice(self.k.late_by)
        if self.k.clock_skew:
            ts += self.rng.uniform(0, self.k.clock_skew)
        return ts

    def _pick_customer(self, ts: float):
        """An existing customer, or an orphan or early-fact id per the knobs."""
        roll = self.rng.random()
        if roll < self.k.orphan_pct:
            return self._take_id("orphan")  # this customer never arrives
        if roll < self.k.orphan_pct + self.k.early_fact_pct:
            customer_id = self._take_id("customer")
            self._at(ts + self.k.late_dim_delay, self._create_customer, customer_id)
            return customer_id
        return self.rng.choice(list(self.customers)) if self.customers else None

    def _active_products(self) -> list[int]:
        return [pid for pid, row in self.products.items() if row["is_active"]]

    # -- rows -------------------------------------------------------------

    def _address(self, city: str, province: str) -> str:
        return f"{self.rng.randint(1, 9999)} {self.rng.choice(STREETS)}, {city}, {province}"

    def _new_customer_row(self, customer_id: int, ts: float) -> dict:
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        city, province, area = self.rng.choice(CITIES)
        row = {
            "customer_id": customer_id,
            "name": f"{first} {last}",
            "email": f"{first}.{last}{customer_id}@{self.rng.choice(EMAIL_DOMAINS)}".lower(),
            "phone": f"{area}-555-{self.rng.randint(0, 9999):04d}",
            "address": self._address(city, province),
            "province": province,
            "loyalty_tier": "bronze",
            "loyalty_points": 0,
            "created_at": iso(ts),
        }
        if self.k.schema_version == "v2":
            row["marketing_opt_in"] = self.rng.random() < 0.4
        return row

    def _new_product_row(self) -> dict:
        category = self.rng.choice(list(CATEGORIES))
        return {
            "product_id": self._take_id("product"),
            "name": f"{self.rng.choice(ADJECTIVES)} {self.rng.choice(CATEGORIES[category])}",
            "category": category,
            "price": round(self.rng.uniform(5, 300), 2),
            "is_active": True,
        }

    def _snapshot(self, ts: float) -> None:
        """Initial load as op='r' rows (CDC-01)."""
        customers, products = self._initial
        for _ in range(products):
            row = self._new_product_row()
            self.products[row["product_id"]] = row
            self._emit("products", self._cdc("products", "r", None, dict(row), ts))
            for warehouse in WAREHOUSES:
                stock = {"product_id": row["product_id"], "warehouse_id": warehouse,
                         "qty_on_hand": self.rng.randint(50, 200), "updated_at": iso(ts)}
                self.inventory[(row["product_id"], warehouse)] = stock
                self._emit("inventory", self._cdc("inventory", "r", None, dict(stock), ts))
        for _ in range(customers):
            row = self._new_customer_row(self._take_id("customer"), ts)
            self.customers[row["customer_id"]] = row
            self._emit("customers", self._cdc("customers", "r", None, dict(row), ts))

    # -- actions ----------------------------------------------------------

    def _random_action(self, ts: float) -> None:
        if self.rng.random() < self.k.bad_record_pct:
            self._bad_record(ts)
            return
        action = self.rng.choices(list(ACTION_WEIGHTS), weights=list(ACTION_WEIGHTS.values()))[0]
        getattr(self, f"_{action}")(ts)

    def _new_customer(self, ts: float) -> None:
        self._create_customer(ts, self._take_id("customer"))

    def _create_customer(self, ts: float, customer_id: int) -> None:
        row = self._new_customer_row(customer_id, ts)
        created = self._cdc("customers", "c", None, dict(row), ts)
        if self.k.delete_before_insert and self.rng.random() < DELETE_BEFORE_INSERT_RATE:
            # The d (higher lsn) lands a batch before its c; the tombstone must win (CDC-05).
            self._emit("customers", self._cdc("customers", "d", dict(row), None, ts))
            self._emit("customers", created, hold=True)
            return
        self.customers[customer_id] = row
        self._emit("customers", created)

    def _customer_changes(self, kind: str, row: dict) -> dict:
        if kind == "tier" and row["loyalty_tier"] == TIERS[-1]:
            kind = "points"
        if kind == "move":
            city, province, _ = self.rng.choice(CITIES)
            return {"address": self._address(city, province), "province": province}
        if kind == "tier":
            return {"loyalty_tier": TIERS[TIERS.index(row["loyalty_tier"]) + 1]}
        return {"loyalty_points": row["loyalty_points"] + self.rng.randint(10, 500)}

    def _customer_change(self, ts: float) -> None:
        if not self.customers:
            return
        customer_id = self.rng.choice(list(self.customers))
        row = self.customers[customer_id]
        kind = self.rng.choices(["move", "tier", "points", "delete"], weights=[40, 20, 30, 10])[0]
        if kind == "delete":  # account closure
            del self.customers[customer_id]
            self._emit("customers", self._cdc("customers", "d", dict(row), None, ts))
            return
        self._update("customers", row, self._customer_changes(kind, row), ts,
                     follow_up=lambda: self._customer_changes("points", row))

    def _price_change(self, row: dict) -> dict:
        return {"price": round(row["price"] * self.rng.uniform(0.75, 1.25), 2)}

    def _product_change(self, ts: float) -> None:
        active = self._active_products()
        if not active:
            return
        row = self.products[self.rng.choice(active)]
        if len(active) > 5 and self.rng.random() < 0.15:
            changes = {"is_active": False}  # discontinued
        else:
            changes = self._price_change(row)
        self._update("products", row, changes, ts, follow_up=lambda: self._price_change(row))

    def _click(self, ts: float) -> None:
        session_id = None
        if self.sessions and self.rng.random() < 0.6:
            session_id = self.rng.choice(list(self.sessions))
            if self.rng.random() < 0.25:  # abandoned
                del self.sessions[session_id]
                session_id = None
        if session_id is None:
            active = self._active_products()
            if not active:
                return
            session_id = self._uuid()
            self.sessions[session_id] = {
                "customer_id": self._pick_customer(ts) if self.rng.random() < 0.7 else None,
                "product_id": self.rng.choice(active),
                "device": self.rng.choice(DEVICES),
                "ip": ".".join(str(self.rng.randint(1, 254)) for _ in range(4)),
                "stage": 0,
            }
        else:
            self.sessions[session_id]["stage"] += 1
        session = self.sessions[session_id]
        event_type = CLICK_FUNNEL[session["stage"]]
        self._emit("clicks", {
            "event_id": self._uuid(),
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "event_type": event_type,
            "product_id": session["product_id"],
            "event_ts": iso(self._event_time(ts)),
            "device": session["device"],
            "ip": session["ip"],
        })
        if event_type == "checkout":
            del self.sessions[session_id]
            if session["customer_id"] is not None:  # order follows within 30 min (JN-06)
                self._at(ts + self.rng.uniform(60, 1800), self._order,
                         session["customer_id"], session["product_id"])

    def _order(self, ts: float, customer_id=None, product_id=None) -> None:
        active = self._active_products()
        if not active:
            return
        if customer_id is None:
            customer_id = self._pick_customer(ts)
        picks = self.rng.sample(active, k=min(len(active), self.rng.randint(1, 3)))
        if product_id in active and product_id not in picks:
            picks[0] = product_id
        items = [{"product_id": pid, "qty": self.rng.randint(1, 3),
                  "unit_price": self.products[pid]["price"]} for pid in picks]
        total = sum(item["qty"] * item["unit_price"] for item in items)
        customer = self.customers.get(customer_id)
        row = {
            "order_id": self._take_id("order"),
            "customer_id": customer_id,
            "items": items,
            "total": round(total, 2),
            "status": "placed",
            "order_ts": iso(self._event_time(ts)),
            "province": customer["province"] if customer else self.rng.choice(CITIES)[1],
        }
        if self.k.schema_version == "v2":
            code = self.rng.choice(DISCOUNT_CODES) if self.rng.random() < 0.25 else None
            row["discount_code"] = code
            if code:
                row["total"] = round(total * (1 - int(re.sub(r"\D", "", code)) / 100), 2)
        self.orders[row["order_id"]] = row
        self._emit("orders", self._cdc("orders", "c", None, dict(row), ts))
        for item in items:
            self._take_stock(ts, item["product_id"], item["qty"])
        if self.rng.random() < 0.08:
            self._at(ts + self.rng.uniform(60, 1200), self._set_status, row["order_id"], "cancelled")
        else:
            self._at(ts + self.rng.uniform(30, 300), self._set_status, row["order_id"], "paid")

    def _set_status(self, ts: float, order_id: int, status: str) -> None:
        row = self.orders.get(order_id)
        if row is None:
            return
        self._update("orders", row, {"status": status}, ts)
        if status == "paid":
            self._at(ts + self.rng.uniform(300, 1800), self._set_status, order_id, "shipped")
        elif status == "shipped":
            self._at(ts + self.rng.uniform(1800, 7200), self._set_status, order_id, "delivered")
        elif status == "delivered" and self.rng.random() < 0.05:
            self._at(ts + self.rng.uniform(600, 3600), self._set_status, order_id, "refunded")
        else:
            del self.orders[order_id]  # final status

    def _take_stock(self, ts: float, product_id: int, qty: int) -> None:
        warehouse = self.rng.choice(WAREHOUSES)
        row = self.inventory[(product_id, warehouse)]
        self._update("inventory", row, {"qty_on_hand": max(0, row["qty_on_hand"] - qty),
                                        "updated_at": iso(ts)}, ts)
        if row["qty_on_hand"] < LOW_STOCK and (product_id, warehouse) not in self._restocking:
            self._restocking.add((product_id, warehouse))
            self._at(ts + self.rng.uniform(600, 3600), self._restock, product_id, warehouse)

    def _restock(self, ts: float, product_id: int, warehouse: str) -> None:
        self._restocking.discard((product_id, warehouse))
        row = self.inventory[(product_id, warehouse)]
        self._update("inventory", row, {"qty_on_hand": row["qty_on_hand"] + self.rng.randint(100, 300),
                                        "updated_at": iso(ts)}, ts)

    def _bad_record(self, ts: float) -> None:
        """A record the pipeline should reject (CDC-08). It never touches the simulated state."""
        kind = self.rng.choice(["null_key", "bad_op", "negative_total", "malformed"])
        if kind in ("null_key", "bad_op"):
            row = self._new_customer_row(0, ts)
            row["customer_id"] = None if kind == "null_key" else self._pick_customer(ts)
            record = self._cdc("customers", "c", None, row, ts)
            if kind == "bad_op":
                record["op"] = "x"
            self._emit("customers", record)
            return
        order = {"order_id": self._take_id("order"), "customer_id": self._pick_customer(ts),
                 "items": [], "total": -round(self.rng.uniform(1, 500), 2), "status": "placed",
                 "order_ts": iso(ts), "province": self.rng.choice(CITIES)[1]}
        record = self._cdc("orders", "c", None, order, ts)
        if kind == "negative_total":
            self._emit("orders", record)
        else:
            line = json.dumps(record, separators=(",", ":"))
            self._emit("orders", line[: self.rng.randint(10, len(line) // 2)])


def write_batch(output_root: str, index: int, batch_ts: float, lines_by_feed: dict[str, list[str]]) -> int:
    """Write one file per non-empty feed. A hidden temp file is renamed into
    place so Auto Loader never reads a half-written file."""
    stamp = datetime.fromtimestamp(batch_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    written = 0
    for feed, lines in lines_by_feed.items():
        if not lines:
            continue
        folder = os.path.join(output_root, FEED_DIRS[feed])
        os.makedirs(folder, exist_ok=True)
        name = f"{stamp}-{index:06d}.json"
        temp = os.path.join(folder, f".{name}.tmp")
        with open(temp, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
        os.replace(temp, os.path.join(folder, name))
        written += len(lines)
    return written


def generate(output_root: str, knobs: Knobs, duration="5m", batch_seconds: float = 5.0,
             start_time: str | None = None, realtime: bool = False,
             initial_customers: int = 100, initial_products: int = 40) -> dict[str, int]:
    """Write a run's files and return the line count per feed."""
    if start_time:
        start = parse_start(start_time)
    else:
        start = time.time() if realtime else parse_start(DEFAULT_START)
    generator = EventGenerator(knobs, start, batch_seconds, initial_customers, initial_products)
    count = max(1, math.ceil(parse_duration(duration) / batch_seconds))
    totals = dict.fromkeys(FEED_DIRS, 0)
    for index, batch_ts, lines in generator.batches(count):
        write_batch(output_root, index, batch_ts, lines)
        for feed, feed_lines in lines.items():
            totals[feed] += len(feed_lines)
        if realtime and index < count - 1:
            time.sleep(batch_seconds)
    summary = ", ".join(f"{feed}={n}" for feed, n in totals.items())
    print(f"Wrote {count} batches to {output_root} (seed {knobs.seed}): {summary}")
    return totals


def scenario_knobs(name: str, overrides: dict | None = None) -> Knobs:
    """A preset's knobs with any overrides applied on top."""
    if name not in SCENARIOS:
        raise ValueError(f"unknown scenario {name!r}; choose from {sorted(SCENARIOS)}")
    return Knobs.from_dict({**SCENARIOS[name]["knobs"], **(overrides or {})})


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Generate seeded e-commerce CDC and click files.")
    parser.add_argument("--output-root", required=True,
                        help="Landing root, e.g. /Volumes/ecom_lab/landing/raw")
    parser.add_argument("--scenario", default="baseline", choices=sorted(SCENARIOS),
                        help="Preset knob set (default baseline: all knobs off)")
    parser.add_argument("--params", default="{}",
                        help='Knob overrides as a JSON object, e.g. \'{"late_pct": "5%%", "seed": 42}\'')
    parser.add_argument("--duration", help="Simulated time to cover: 90s, 10m, 1h (default: the scenario's)")
    parser.add_argument("--batch-seconds", type=float, default=5.0)
    parser.add_argument("--start-time",
                        help=f"ISO-8601 start of simulated time (default {DEFAULT_START}, or now with --realtime)")
    parser.add_argument("--realtime", action="store_true", help="Sleep between batches so files trickle in")
    parser.add_argument("--initial-customers", type=int, default=100)
    parser.add_argument("--initial-products", type=int, default=40)
    args = parser.parse_args(argv)

    generate(args.output_root, scenario_knobs(args.scenario, json.loads(args.params)),
             args.duration or SCENARIOS[args.scenario]["duration"], args.batch_seconds,
             args.start_time, args.realtime, args.initial_customers, args.initial_products)


if __name__ == "__main__":
    main()
