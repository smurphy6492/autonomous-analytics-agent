"""Generate realistic synthetic marketing/web analytics dataset.

Produces 4 CSV files in data/raw/marketing/:
  - sessions.csv: ~200K sessions over 12 months with traffic source, device, landing page
  - pageviews.csv: ~800K pageviews tied to sessions
  - transactions.csv: ~15K purchases with revenue, tied to sessions
  - campaigns.csv: paid campaign metadata with spend

Models a B2C e-commerce site with realistic patterns:
  - Channel mix: organic > paid_search > social > email > referral > direct
  - Funnel drop-offs: session → product view → add to cart → checkout → purchase
  - Seasonal spikes (Black Friday, holiday)
  - Mobile vs desktop conversion gap
  - Channel-specific conversion rates
"""

from __future__ import annotations

import csv
import random
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "marketing"
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)

CHANNELS = {
    "organic_search": {"weight": 30, "cvr": 0.032, "bounce": 0.40},
    "paid_search": {"weight": 22, "cvr": 0.045, "bounce": 0.35},
    "social": {"weight": 18, "cvr": 0.012, "bounce": 0.55},
    "email": {"weight": 12, "cvr": 0.055, "bounce": 0.25},
    "referral": {"weight": 10, "cvr": 0.028, "bounce": 0.38},
    "direct": {"weight": 8, "cvr": 0.038, "bounce": 0.30},
}

DEVICES = {
    "desktop": {"weight": 45, "cvr_mult": 1.0},
    "mobile": {"weight": 45, "cvr_mult": 0.55},
    "tablet": {"weight": 10, "cvr_mult": 0.75},
}

LANDING_PAGES = [
    "/", "/products", "/products/category/electronics", "/products/category/clothing",
    "/products/category/home", "/sale", "/blog", "/blog/gift-guide",
    "/about", "/products/category/accessories",
]

PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Accessories", "Sports", "Books", "Beauty"]

def _build_campaigns() -> list[dict]:
    """Build campaign list with pre-computed total_spend."""
    raw = [
        {"campaign_id": "camp_001", "campaign_name": "Spring Sale 2025", "channel": "paid_search", "start_date": "2025-03-01", "end_date": "2025-03-31", "daily_budget": 500},
        {"campaign_id": "camp_002", "campaign_name": "Summer Push", "channel": "paid_search", "start_date": "2025-06-01", "end_date": "2025-08-31", "daily_budget": 750},
        {"campaign_id": "camp_003", "campaign_name": "Back to School", "channel": "social", "start_date": "2025-08-15", "end_date": "2025-09-15", "daily_budget": 400},
        {"campaign_id": "camp_004", "campaign_name": "Black Friday", "channel": "paid_search", "start_date": "2025-11-20", "end_date": "2025-12-02", "daily_budget": 2000},
        {"campaign_id": "camp_005", "campaign_name": "Holiday Email", "channel": "email", "start_date": "2025-11-15", "end_date": "2025-12-25", "daily_budget": 200},
        {"campaign_id": "camp_006", "campaign_name": "Social Retargeting", "channel": "social", "start_date": "2025-01-01", "end_date": "2025-12-31", "daily_budget": 150},
        {"campaign_id": "camp_007", "campaign_name": "Brand Search", "channel": "paid_search", "start_date": "2025-01-01", "end_date": "2025-12-31", "daily_budget": 300},
    ]
    for c in raw:
        days = (date.fromisoformat(c["end_date"]) - date.fromisoformat(c["start_date"])).days + 1
        c["days_active"] = days
        c["total_spend"] = c["daily_budget"] * days
    return raw


CAMPAIGNS = _build_campaigns()


def seasonal_multiplier(d: date) -> float:
    """Traffic multiplier based on time of year."""
    month = d.month
    day = d.day
    # Black Friday week
    if month == 11 and 24 <= day <= 30:
        return 3.0
    # December holiday shopping
    if month == 12 and day <= 20:
        return 2.0
    # January slump
    if month == 1:
        return 0.75
    # Summer dip
    if month in (7, 8):
        return 0.85
    return 1.0


def day_of_week_multiplier(d: date) -> float:
    dow = d.weekday()
    if dow >= 5:
        return 1.2  # weekends slightly higher for B2C
    if dow == 0:
        return 0.9  # Monday dip
    return 1.0


def main() -> None:
    random.seed(SEED)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    channel_names = list(CHANNELS.keys())
    channel_weights = [CHANNELS[c]["weight"] for c in channel_names]
    device_names = list(DEVICES.keys())
    device_weights = [DEVICES[d]["weight"] for d in device_names]

    sessions = []
    pageviews = []
    transactions = []
    session_id = 0
    pv_id = 0
    txn_id = 0
    next_user_id = 1

    # Recent user buffer for return visits (fixed size, fast sampling)
    recent_users: deque[str] = deque(maxlen=5000)

    base_daily_sessions = 600
    current = START_DATE

    def pick_user_id() -> str:
        nonlocal next_user_id
        # 35% of sessions are returning users (if buffer has users)
        if recent_users and random.random() < 0.35:
            return random.choice(recent_users)
        # New user
        uid = f"user_{next_user_id:06d}"
        next_user_id += 1
        recent_users.append(uid)
        return uid

    print("Generating marketing dataset...")

    while current <= END_DATE:
        daily_sessions = int(
            base_daily_sessions
            * seasonal_multiplier(current)
            * day_of_week_multiplier(current)
            * random.gauss(1.0, 0.1)
        )

        for _ in range(daily_sessions):
            session_id += 1
            user_id = pick_user_id()
            channel = random.choices(channel_names, weights=channel_weights, k=1)[0]
            device = random.choices(device_names, weights=device_weights, k=1)[0]
            landing = random.choice(LANDING_PAGES)

            # Bounce?
            bounce_rate = CHANNELS[channel]["bounce"]
            if device == "mobile":
                bounce_rate *= 1.15
            bounced = random.random() < bounce_rate

            # Session duration and pages
            if bounced:
                pages = 1
                duration_sec = random.randint(3, 30)
            else:
                pages = random.randint(2, 12)
                duration_sec = random.randint(30, 900)

            # Hour of day (peak 10am-9pm)
            hour = random.choices(
                range(24),
                weights=[1, 1, 1, 1, 1, 2, 3, 5, 7, 9, 10, 10, 9, 9, 8, 8, 9, 10, 10, 9, 7, 5, 3, 2],
                k=1,
            )[0]
            session_start = datetime(current.year, current.month, current.day, hour, random.randint(0, 59), random.randint(0, 59))

            # Assign campaign if applicable
            campaign_id = ""
            if channel in ("paid_search", "social", "email"):
                for camp in CAMPAIGNS:
                    if camp["channel"] == channel:
                        cs = date.fromisoformat(camp["start_date"])
                        ce = date.fromisoformat(camp["end_date"])
                        if cs <= current <= ce:
                            campaign_id = camp["campaign_id"]
                            break

            sessions.append({
                "session_id": f"sess_{session_id:07d}",
                "user_id": user_id,
                "session_date": current.isoformat(),
                "session_start": session_start.isoformat(),
                "channel": channel,
                "device": device,
                "landing_page": landing,
                "pages_viewed": pages,
                "duration_seconds": duration_sec,
                "bounced": 1 if bounced else 0,
                "campaign_id": campaign_id,
                "country": random.choices(
                    ["US", "UK", "CA", "DE", "AU", "FR"],
                    weights=[50, 15, 10, 8, 7, 10],
                    k=1,
                )[0],
            })

            # Generate pageviews
            viewed_pages = [landing]
            for _ in range(pages - 1):
                viewed_pages.append(random.choice(LANDING_PAGES))
            for i, page in enumerate(viewed_pages):
                pv_id += 1
                pageviews.append({
                    "pageview_id": f"pv_{pv_id:08d}",
                    "session_id": f"sess_{session_id:07d}",
                    "page_url": page,
                    "page_number": i + 1,
                    "time_on_page_seconds": random.randint(5, 180) if i < len(viewed_pages) - 1 else 0,
                })

            # Conversion
            if not bounced:
                cvr = CHANNELS[channel]["cvr"] * DEVICES[device]["cvr_mult"]
                # Seasonal boost to conversion
                if current.month in (11, 12):
                    cvr *= 1.3
                if random.random() < cvr:
                    txn_id += 1
                    num_items = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1)[0]
                    avg_item_price = random.gauss(55, 25)
                    revenue = round(max(10, num_items * max(5, avg_item_price)), 2)
                    transactions.append({
                        "transaction_id": f"txn_{txn_id:06d}",
                        "session_id": f"sess_{session_id:07d}",
                        "transaction_date": current.isoformat(),
                        "revenue": revenue,
                        "items": num_items,
                        "category": random.choice(PRODUCT_CATEGORIES),
                        "coupon_used": 1 if random.random() < 0.2 else 0,
                    })

        current += timedelta(days=1)

    # Write sessions
    path = OUTPUT_DIR / "sessions.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sessions[0].keys())
        writer.writeheader()
        writer.writerows(sessions)
    print(f"  sessions.csv: {len(sessions)} rows")

    # Write pageviews
    path = OUTPUT_DIR / "pageviews.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=pageviews[0].keys())
        writer.writeheader()
        writer.writerows(pageviews)
    print(f"  pageviews.csv: {len(pageviews)} rows")

    # Write transactions
    path = OUTPUT_DIR / "transactions.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=transactions[0].keys())
        writer.writeheader()
        writer.writerows(transactions)
    print(f"  transactions.csv: {len(transactions)} rows")

    # Write campaigns
    path = OUTPUT_DIR / "campaigns.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CAMPAIGNS[0].keys())
        writer.writeheader()
        writer.writerows(CAMPAIGNS)
    print(f"  campaigns.csv: {len(CAMPAIGNS)} rows")

    print(f"\nDone. Files written to {OUTPUT_DIR}")
    print(f"Conversion rate: {len(transactions) / len(sessions) * 100:.2f}%")


if __name__ == "__main__":
    main()
