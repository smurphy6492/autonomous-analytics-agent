"""Generate realistic synthetic SaaS subscription dataset.

Produces 5 CSV files in data/raw/saas/:
  - plans.csv: pricing tiers (Starter, Pro, Enterprise)
  - customers.csv: ~10K customers with signup dates and metadata
  - subscriptions.csv: subscription lifecycle events (created, upgraded, downgraded, churned)
  - invoices.csv: monthly billing records with MRR
  - events.csv: daily product usage (logins, API calls, features used)

Data spans 24 months (2024-01 through 2025-12) with realistic patterns:
  - Seasonal churn spikes (Jan, post-trial)
  - Cohort decay curves
  - Expansion revenue from upgrades
  - Usage-correlated churn (low usage → higher churn probability)
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "saas"
START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)
NUM_CUSTOMERS = 10_000

PLANS = [
    {"plan_id": "starter", "plan_name": "Starter", "monthly_price": 29, "annual_price": 290, "seat_limit": 3, "api_limit": 1000},
    {"plan_id": "pro", "plan_name": "Pro", "monthly_price": 99, "annual_price": 990, "seat_limit": 15, "api_limit": 10000},
    {"plan_id": "enterprise", "plan_name": "Enterprise", "monthly_price": 349, "annual_price": 3490, "seat_limit": 999, "api_limit": 100000},
]

PLAN_PRICES = {p["plan_id"]: p["monthly_price"] for p in PLANS}
PLAN_IDS = [p["plan_id"] for p in PLANS]

INDUSTRIES = ["Technology", "Healthcare", "Finance", "Retail", "Education", "Manufacturing", "Media", "Real Estate"]
COMPANY_SIZES = ["1-10", "11-50", "51-200", "201-1000", "1001+"]
SOURCES = ["organic", "paid_search", "referral", "partner", "outbound"]
FEATURES = ["dashboard", "reports", "api", "integrations", "alerts", "exports", "collaboration", "advanced_analytics"]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def monthly_churn_rate(plan_id: str, month: int, months_active: int, avg_daily_logins: float) -> float:
    """Churn probability for a given month. Depends on plan, tenure, season, and usage."""
    base = {"starter": 0.06, "pro": 0.03, "enterprise": 0.01}[plan_id]

    # New customers churn more (first 3 months)
    if months_active <= 3:
        base *= 1.8

    # January churn spike (budget resets)
    if month == 1:
        base *= 1.4

    # Low usage = higher churn
    if avg_daily_logins < 1.0:
        base *= 2.0
    elif avg_daily_logins > 5.0:
        base *= 0.4

    return min(base, 0.25)


def generate_plans() -> None:
    path = OUTPUT_DIR / "plans.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PLANS[0].keys())
        writer.writeheader()
        writer.writerows(PLANS)
    print(f"  plans.csv: {len(PLANS)} rows")


def generate_customers() -> list[dict]:
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup = random_date(START_DATE, END_DATE - timedelta(days=30))
        industry = random.choice(INDUSTRIES)
        size = random.choices(
            COMPANY_SIZES,
            weights=[30, 30, 20, 15, 5],
            k=1,
        )[0]
        source = random.choices(
            SOURCES,
            weights=[30, 25, 20, 15, 10],
            k=1,
        )[0]

        # Plan distribution: 50% starter, 35% pro, 15% enterprise
        # Larger companies skew toward enterprise
        if size in ("201-1000", "1001+"):
            plan = random.choices(PLAN_IDS, weights=[15, 35, 50], k=1)[0]
        elif size in ("51-200",):
            plan = random.choices(PLAN_IDS, weights=[25, 50, 25], k=1)[0]
        else:
            plan = random.choices(PLAN_IDS, weights=[55, 35, 10], k=1)[0]

        customers.append({
            "customer_id": f"cust_{i:05d}",
            "company_name": f"Company_{i}",
            "industry": industry,
            "company_size": size,
            "signup_date": signup.isoformat(),
            "acquisition_source": source,
            "initial_plan": plan,
            "country": random.choices(
                ["US", "UK", "CA", "DE", "AU", "FR", "BR", "IN"],
                weights=[40, 12, 10, 8, 7, 6, 5, 12],
                k=1,
            )[0],
        })

    path = OUTPUT_DIR / "customers.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
    print(f"  customers.csv: {len(customers)} rows")
    return customers


def generate_subscriptions_and_invoices(customers: list[dict]) -> tuple[list[dict], list[dict]]:
    subscriptions = []
    invoices = []
    sub_id = 0
    inv_id = 0

    for cust in customers:
        cust_id = cust["customer_id"]
        current_plan = cust["initial_plan"]
        signup = date.fromisoformat(cust["signup_date"])

        # Trial period: 14 days, ~60% convert
        trial_end = signup + timedelta(days=14)
        if random.random() > 0.60:
            # Didn't convert from trial
            sub_id += 1
            subscriptions.append({
                "subscription_id": f"sub_{sub_id:06d}",
                "customer_id": cust_id,
                "plan_id": current_plan,
                "status": "trial_expired",
                "started_at": signup.isoformat(),
                "ended_at": trial_end.isoformat(),
                "mrr": 0,
            })
            continue

        # Converted from trial
        sub_start = trial_end
        sub_id += 1
        active_sub_id = f"sub_{sub_id:06d}"

        # Walk through months
        current_date = sub_start.replace(day=1)
        months_active = 0
        churned = False
        avg_daily_logins = random.gauss(3.5, 2.0)
        avg_daily_logins = max(0.1, avg_daily_logins)

        while current_date <= END_DATE and not churned:
            months_active += 1
            month_end = (current_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            mrr = PLAN_PRICES[current_plan]

            # Invoice
            inv_id += 1
            invoices.append({
                "invoice_id": f"inv_{inv_id:07d}",
                "customer_id": cust_id,
                "subscription_id": active_sub_id,
                "plan_id": current_plan,
                "invoice_date": current_date.isoformat(),
                "amount": mrr,
                "status": "paid",
            })

            # Check for churn
            churn_prob = monthly_churn_rate(current_plan, current_date.month, months_active, avg_daily_logins)
            if random.random() < churn_prob and months_active >= 2:
                churned = True
                subscriptions.append({
                    "subscription_id": active_sub_id,
                    "customer_id": cust_id,
                    "plan_id": current_plan,
                    "status": "churned",
                    "started_at": sub_start.isoformat(),
                    "ended_at": month_end.isoformat(),
                    "mrr": mrr,
                })
                break

            # Check for plan change (5% upgrade, 2% downgrade per month for eligible plans)
            if months_active >= 3 and not churned:
                plan_idx = PLAN_IDS.index(current_plan)
                if plan_idx < 2 and random.random() < 0.05:
                    old_plan = current_plan
                    current_plan = PLAN_IDS[plan_idx + 1]
                    sub_id += 1
                    subscriptions.append({
                        "subscription_id": active_sub_id,
                        "customer_id": cust_id,
                        "plan_id": old_plan,
                        "status": "upgraded",
                        "started_at": sub_start.isoformat(),
                        "ended_at": current_date.isoformat(),
                        "mrr": PLAN_PRICES[old_plan],
                    })
                    active_sub_id = f"sub_{sub_id:06d}"
                    sub_start = current_date
                elif plan_idx > 0 and random.random() < 0.02:
                    old_plan = current_plan
                    current_plan = PLAN_IDS[plan_idx - 1]
                    sub_id += 1
                    subscriptions.append({
                        "subscription_id": active_sub_id,
                        "customer_id": cust_id,
                        "plan_id": old_plan,
                        "status": "downgraded",
                        "started_at": sub_start.isoformat(),
                        "ended_at": current_date.isoformat(),
                        "mrr": PLAN_PRICES[old_plan],
                    })
                    active_sub_id = f"sub_{sub_id:06d}"
                    sub_start = current_date

            # Advance to next month
            current_date = (current_date.replace(day=28) + timedelta(days=4)).replace(day=1)

        # If still active at end of period
        if not churned:
            subscriptions.append({
                "subscription_id": active_sub_id,
                "customer_id": cust_id,
                "plan_id": current_plan,
                "status": "active",
                "started_at": sub_start.isoformat(),
                "ended_at": "",
                "mrr": PLAN_PRICES[current_plan],
            })

    path = OUTPUT_DIR / "subscriptions.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=subscriptions[0].keys())
        writer.writeheader()
        writer.writerows(subscriptions)
    print(f"  subscriptions.csv: {len(subscriptions)} rows")

    path = OUTPUT_DIR / "invoices.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=invoices[0].keys())
        writer.writeheader()
        writer.writerows(invoices)
    print(f"  invoices.csv: {len(invoices)} rows")

    return subscriptions, invoices


def generate_events(customers: list[dict], subscriptions: list[dict]) -> None:
    """Generate daily usage events for active customers."""
    # Build a map of customer → active date ranges
    active_ranges: dict[str, list[tuple[date, date]]] = {}
    for sub in subscriptions:
        if sub["status"] in ("trial_expired",):
            continue
        cust_id = sub["customer_id"]
        start = date.fromisoformat(sub["started_at"])
        end = date.fromisoformat(sub["ended_at"]) if sub["ended_at"] else END_DATE
        if cust_id not in active_ranges:
            active_ranges[cust_id] = []
        active_ranges[cust_id].append((start, end))

    events = []
    event_id = 0

    # Sample ~20% of customers for daily events (full dataset would be too large)
    sampled_customers = random.sample(
        [c for c in customers if c["customer_id"] in active_ranges],
        min(2000, len(active_ranges)),
    )

    for cust in sampled_customers:
        cust_id = cust["customer_id"]
        base_logins = max(0.5, random.gauss(3.0, 1.5))
        base_api_calls = max(0, random.gauss(50, 30))

        for start, end in active_ranges[cust_id]:
            current = start
            while current <= end:
                # Skip weekends (lower usage)
                is_weekend = current.weekday() >= 5
                day_factor = 0.3 if is_weekend else 1.0

                logins = max(0, int(random.gauss(base_logins * day_factor, 1.0)))
                api_calls = max(0, int(random.gauss(base_api_calls * day_factor, 15)))
                features_used = random.sample(FEATURES, k=min(random.randint(1, 5), len(FEATURES)))

                if logins > 0 or api_calls > 0:
                    event_id += 1
                    events.append({
                        "event_id": f"evt_{event_id:08d}",
                        "customer_id": cust_id,
                        "event_date": current.isoformat(),
                        "logins": logins,
                        "api_calls": api_calls,
                        "features_used": len(features_used),
                        "feature_list": "|".join(features_used),
                    })

                current += timedelta(days=1)

    path = OUTPUT_DIR / "events.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=events[0].keys())
        writer.writeheader()
        writer.writerows(events)
    print(f"  events.csv: {len(events)} rows")


def main() -> None:
    random.seed(SEED)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating SaaS dataset...")
    generate_plans()
    customers = generate_customers()
    subscriptions, _invoices = generate_subscriptions_and_invoices(customers)
    generate_events(customers, subscriptions)
    print(f"\nDone. Files written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
