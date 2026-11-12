"""Synthetic retail source data with realistic problems baked in.

Standard library only, deterministic for a given seed. It writes what a source
system would drop into a landing zone:

    landing/customers/customers.csv        full snapshot, mixed date formats, dupes, bad emails
    landing/products/products.csv
    landing/stores/stores.csv
    landing/orders/dt=YYYY-MM-DD/orders.jsonl
        one file per day; nested line items; exact duplicates; bad timestamps;
        negative quantities; unknown products/customers; and status updates for
        the previous day's orders (late-arriving changes).
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

FIRST = ["Aarav", "Vivaan", "Aditi", "Ishita", "Karthik", "Meera", "Rohan", "Sneha", "Arjun", "Divya", "Nikhil", "Priya"]
LAST = ["Sharma", "Iyer", "Reddy", "Patel", "Nair", "Gupta", "Khan", "Das", "Menon", "Rao"]
CITIES = [
    ("Chennai", "South"), ("Bengaluru", "South"), ("Hyderabad", "South"), ("Mumbai", "West"),
    ("Pune", "West"), ("Ahmedabad", "West"), ("Delhi", "North"), ("Kolkata", "East"),
]
SEGMENTS = ["Regular", "Premium", "New"]
CATEGORIES = {
    "Beverages": ["AquaPure", "ChaiCo", "Fizzo"],
    "Snacks": ["Crunchy", "NuttyBar", "ChipChip"],
    "Dairy": ["FreshMoo", "CurdKing"],
    "Personal Care": ["Glow", "PureSkin"],
    "Household": ["SparkleClean", "HomeEase"],
}
STORE_FORMATS = ["Hypermarket", "Supermarket", "Express"]
STATUS_FLOW = {"placed": ["shipped", "cancelled"], "shipped": ["delivered"]}


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _make_customers(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        email = f"{first}.{last}{i}@example.com".lower()
        r = rng.random()
        if r < 0.03:
            email = ""
        elif r < 0.05:
            email = "not-an-email"
        elif r < 0.15:
            email = email.upper()
        name = f"{first} {last}"
        if rng.random() < 0.05:
            name = f"  {name.lower()} "
        signup = date(2024, 1, 1) + timedelta(days=rng.randint(0, 700))
        signup_str = signup.strftime("%d/%m/%Y") if rng.random() < 0.15 else signup.isoformat()
        rows.append(
            {
                "customer_id": f"C{i:06d}",
                "name": name,
                "email": email,
                "city": rng.choice(CITIES)[0],
                "segment": rng.choice(SEGMENTS),
                "signup_date": signup_str,
            }
        )
    dupes = rng.sample(rows, k=max(1, int(n * 0.02)))
    rows.extend(dict(d, name=d["name"].strip().upper()) for d in dupes)
    rng.shuffle(rows)
    return rows


def _make_products(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        rows.append(
            {
                "product_id": f"P{i:05d}",
                "sku": f"sku-{i:05d}",
                "category": category.lower(),
                "brand": rng.choice(CATEGORIES[category]),
                "mrp": f"{rng.uniform(20, 2000):.2f}",
            }
        )
    return rows


def _make_stores(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        city, region = rng.choice(CITIES)
        rows.append(
            {"store_id": f"S{i:03d}", "city": city, "region": region, "format": rng.choice(STORE_FORMATS)}
        )
    return rows


def _make_order(rng, order_id, day, customer_ids, store_ids, products) -> dict:
    ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(8 * 3600, 22 * 3600))
    r = rng.random()
    if r < 0.01:
        order_ts = "N/A"
    elif r < 0.04:
        order_ts = ts.strftime("%Y-%m-%d %H:%M:%S")
    else:
        order_ts = ts.isoformat()

    r = rng.random()
    if r < 0.02:
        customer_id = None
    elif r < 0.03:
        customer_id = "C999999"  # not in the customer master
    else:
        customer_id = rng.choice(customer_ids)

    items = []
    for _ in range(rng.randint(1, 5)):
        product_id, mrp = rng.choice(products)
        if rng.random() < 0.01:
            product_id = "P99999"  # unknown product
        qty = -1 if rng.random() < 0.01 else rng.randint(1, 6)
        discount = 150 if rng.random() < 0.005 else rng.choice([0, 0, 0, 5, 10, 15])
        items.append(
            {
                "product_id": product_id,
                "qty": qty,
                "unit_price": round(mrp * rng.uniform(0.9, 1.0), 2),
                "discount_pct": discount,
            }
        )

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "store_id": rng.choice(store_ids),
        "order_ts": order_ts,
        "updated_at": ts.isoformat(),
        "status": rng.choices(["placed", "shipped", "delivered", "cancelled"], weights=[50, 30, 15, 5])[0],
        "items": items,
    }


def generate(gen_cfg: dict, landing: str | Path) -> dict:
    """Write all landing files and return a summary of what was produced."""
    landing = Path(landing)
    rng = random.Random(gen_cfg["seed"])
    start = date.fromisoformat(str(gen_cfg["start_date"]))

    customers = _make_customers(rng, gen_cfg["customers"])
    products = _make_products(rng, gen_cfg["products"])
    stores = _make_stores(rng, gen_cfg["stores"])
    _write_csv(landing / "customers" / "customers.csv", customers)
    _write_csv(landing / "products" / "products.csv", products)
    _write_csv(landing / "stores" / "stores.csv", stores)

    customer_ids = [c["customer_id"] for c in customers]
    store_ids = [s["store_id"] for s in stores]
    price_list = [(p["product_id"], float(p["mrp"])) for p in products]

    total_rows = 0
    prior: list[dict] = []
    for d in range(gen_cfg["days"]):
        day = start + timedelta(days=d)
        lines: list[dict] = []
        todays: list[dict] = []
        n_orders = int(gen_cfg["orders_per_day"] * rng.uniform(0.8, 1.2))
        for i in range(n_orders):
            order = _make_order(rng, f"O{day:%Y%m%d}{i:05d}", day, customer_ids, store_ids, price_list)
            todays.append(order)
            lines.append(order)
            if rng.random() < 0.02:
                lines.append(dict(order))  # exact duplicate delivery

        # Late-arriving status changes for yesterday's orders.
        for old in rng.sample(prior, k=int(len(prior) * 0.05)):
            next_states = STATUS_FLOW.get(old["status"])
            if not next_states:
                continue
            update = dict(old)
            update["status"] = rng.choice(next_states)
            later = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(9 * 3600, 20 * 3600))
            update["updated_at"] = later.isoformat()
            lines.append(update)

        prior = todays
        out = landing / "orders" / f"dt={day.isoformat()}" / "orders.jsonl"
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")
        total_rows += len(lines)

    return {
        "customers": len(customers),
        "products": len(products),
        "stores": len(stores),
        "order_files": gen_cfg["days"],
        "order_rows": total_rows,
    }
