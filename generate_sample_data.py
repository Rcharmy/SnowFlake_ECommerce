"""
generate_sample_data.py
-----------------------
Generates realistic e-commerce sample data files for the pipeline:
  - orders.csv          (10,000 rows)
  - products.csv        (500 rows)
  - customers.json      (2,000 rows)
  - web_events.json     (50,000 rows)

Run: python generate_sample_data.py
Output files land in the same directory.
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ── CONFIG ──────────────────────────────────────────────────
N_CUSTOMERS  = 2_000
N_PRODUCTS   = 500
N_ORDERS     = 10_000
N_WEB_EVENTS = 50_000

CATEGORIES   = ["Electronics", "Apparel", "Home & Garden", "Sports", "Beauty", "Books", "Toys"]
REGIONS      = ["North America", "Europe", "Asia Pacific", "Latin America"]
SEGMENTS     = ["VIP", "Regular", "At-Risk", "New"]
EVENT_TYPES  = ["page_view", "add_to_cart", "checkout_start", "purchase", "search"]
STATUSES     = ["completed", "shipped", "returned", "cancelled"]

START_DATE   = datetime(2023, 1, 1)
END_DATE     = datetime(2024, 12, 31)

def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return (start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))).isoformat()

# ── CUSTOMERS ───────────────────────────────────────────────
customers = []
for i in range(1, N_CUSTOMERS + 1):
    customers.append({
        "customer_id":  f"CUST_{i:05d}",
        "email":        f"user{i}@example.com",
        "first_name":   random.choice(["Alice","Bob","Carol","David","Emma","Frank","Grace","Hiro","Ivy","James"]),
        "last_name":    random.choice(["Smith","Johnson","Lee","Garcia","Chen","Patel","Brown","Wilson","Kim","Davis"]),
        "region":       random.choice(REGIONS),
        "segment":      random.choice(SEGMENTS),
        "signup_date":  rand_date(START_DATE, END_DATE - timedelta(days=90)),
        "is_active":    random.choice([True, True, True, False])
    })

with open("customers.json", "w") as f:
    json.dump(customers, f, indent=2)
print(f"✓ customers.json  ({N_CUSTOMERS} records)")

# ── PRODUCTS ────────────────────────────────────────────────
products = []
for i in range(1, N_PRODUCTS + 1):
    category   = random.choice(CATEGORIES)
    base_price = round(random.uniform(5.0, 999.99), 2)
    products.append({
        "product_id":    f"PROD_{i:04d}",
        "product_name":  f"{category} Item {i}",
        "category":      category,
        "sub_category":  f"{category} - Sub {random.randint(1,5)}",
        "unit_price":    base_price,
        "price_tier":    "Budget" if base_price < 30 else ("Mid" if base_price < 150 else "Premium"),
        "in_stock":      random.choice([True, True, True, False]),
        "supplier_id":   f"SUP_{random.randint(1,50):03d}"
    })

with open("products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=products[0].keys())
    writer.writeheader()
    writer.writerows(products)
print(f"✓ products.csv    ({N_PRODUCTS} records)")

# ── ORDERS ──────────────────────────────────────────────────
orders = []
for i in range(1, N_ORDERS + 1):
    product   = random.choice(products)
    customer  = random.choice(customers)
    qty       = random.randint(1, 10)
    discount  = round(random.uniform(0, 0.3), 2)
    revenue   = round(product["unit_price"] * qty * (1 - discount), 2)
    orders.append({
        "order_id":        f"ORD_{i:06d}",
        "customer_id":     customer["customer_id"],
        "product_id":      product["product_id"],
        "order_date":      rand_date(),
        "quantity":        qty,
        "unit_price":      product["unit_price"],
        "discount_pct":    discount,
        "revenue":         revenue,
        "order_status":    random.choice(STATUSES),
        "shipping_region": customer["region"],
        "payment_method":  random.choice(["credit_card", "paypal", "bank_transfer", "crypto"])
    })

with open("orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)
print(f"✓ orders.csv      ({N_ORDERS} records)")

# ── WEB EVENTS ──────────────────────────────────────────────
web_events = []
for i in range(1, N_WEB_EVENTS + 1):
    customer = random.choice(customers)
    product  = random.choice(products)
    web_events.append({
        "event_id":      str(uuid.uuid4()),
        "customer_id":   customer["customer_id"],
        "product_id":    product["product_id"] if random.random() > 0.3 else None,
        "event_type":    random.choice(EVENT_TYPES),
        "event_ts":      rand_date(),
        "session_id":    str(uuid.uuid4())[:8],
        "device":        random.choice(["mobile", "desktop", "tablet"]),
        "page_url":      f"/{'shop' if random.random()>0.3 else 'product'}/{product['category'].lower().replace(' ','_')}"
    })

with open("web_events.json", "w") as f:
    json.dump(web_events, f, indent=2)
print(f"✓ web_events.json ({N_WEB_EVENTS} records)")
print("\nAll sample data files generated. Upload to S3 to begin the pipeline.")
