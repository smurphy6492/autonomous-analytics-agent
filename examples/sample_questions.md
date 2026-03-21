# Sample Business Questions — Olist E-Commerce Dataset

Questions organized by category for testing and demo purposes. Use alongside `docs/eval-framework.md` to score agent responses.

---

## Category 1: Straightforward Lookup

These questions have a single correct answer derivable from the data. The agent should always get these right. Use as a baseline sanity check.

1. **What was the total revenue for 2017?**
   - Expected behavior: Query `order_payments` filtered to 2017, sum `payment_value`
   - Pass: Returns a specific dollar figure with correct SQL logic

2. **What are the top 5 product categories by number of orders?**
   - Expected behavior: Join `order_items` → `products`, group by category, count orders
   - Pass: Returns a ranked list with counts

3. **What is the average review score across all orders?**
   - Expected behavior: Simple aggregate on `order_reviews.review_score`
   - Pass: Returns a number between 1.0 and 5.0

4. **Which state has the most customers?**
   - Expected behavior: Group `customers` by `customer_state`, count, sort descending
   - Pass: Returns a state abbreviation (likely SP — São Paulo)

---

## Category 2: Ambiguous Intent

These questions have multiple valid interpretations. The agent should either ask a clarifying question or explicitly state which interpretation it chose and why.

5. **Who are our best customers?**
   - Ambiguity: "Best" could mean highest total spend, most orders, most recent purchase, highest review scores given, or longest tenure
   - Expected behavior: Ask which definition of "best" to use, OR answer with one definition and explicitly flag the assumption
   - Failure mode: Picks one interpretation silently with no acknowledgment

6. **How is the business performing?**
   - Ambiguity: No time frame, no metric specified, no comparison baseline
   - Expected behavior: Ask for clarification on time period and success metric, OR explicitly scope the answer and state what was assumed
   - Failure mode: Returns a broad generic summary without flagging that the question was underspecified

7. **Which sellers are underperforming?**
   - Ambiguity: "Underperforming" relative to what? Average? Prior period? A threshold? Revenue or review score or delivery time?
   - Expected behavior: Surface the ambiguity and ask, or answer with one metric and label it clearly
   - Failure mode: Answers confidently using an arbitrary metric without disclosure

8. **What is our customer retention rate?**
   - Ambiguity: Olist data has repeat customers but no formal cohort tracking. "Retention" could mean repeat purchase rate, which requires a definition window (30/60/90 days, same year, etc.)
   - Expected behavior: Flag that retention requires a time window definition, propose one, and answer accordingly
   - Failure mode: Returns a number without explaining the methodology

---

## Category 3: Multi-Step Reasoning

These questions require the agent to combine multiple analyses or think across more than one table or time dimension. Tests analytical depth.

9. **Why did revenue decline in late 2018, and which product categories drove the change?**
   - Requires: Time series analysis, period-over-period comparison, category-level decomposition
   - Expected behavior: Identifies the decline period, quantifies it, breaks down by category, offers a data-backed hypothesis
   - Pass: Produces at least two queries (trend + breakdown) and connects the findings in the summary

10. **What is the relationship between delivery time and review score, and does it vary by region?**
    - Requires: Join `orders` (for delivery timestamps) + `reviews` + `customers` (for state/region), calculate delivery duration, correlate with score, segment by geography
    - Expected behavior: Calculates actual vs. estimated delivery delta, shows review score distribution by delivery time bucket, segments by state
    - Pass: Surfaces a clear pattern (e.g., late deliveries consistently score lower) with regional nuance

11. **Which product categories have the highest return on freight — i.e., where does high freight cost not hurt conversion or review scores?**
    - Requires: Join `order_items` (freight), `products` (category), `reviews` (score), calculate freight as % of order value, cross-reference with review scores
    - Expected behavior: Produces a category-level summary table with freight %, avg review score, and order volume
    - Pass: Identifies categories where customers tolerate high freight (likely heavy/bulky goods)

---

## Category 4: Truly Unanswerable

These questions cannot be answered with the Olist dataset because the required data does not exist. The agent should clearly state this rather than fabricate an answer.

12. **What is our customer acquisition cost (CAC)?**
    - Why unanswerable: No marketing spend, ad campaign, or channel attribution data in Olist
    - Expected behavior: Explains that CAC requires marketing spend data which is not present in this dataset. May offer a proxy (e.g., order volume growth as a demand signal) if appropriate.
    - Failure mode: Returns a fabricated number or confuses revenue per customer with acquisition cost

13. **What is the profit margin by product category?**
    - Why unanswerable: Olist contains revenue (payment_value) and freight cost, but not cost of goods sold (COGS) or seller margin data
    - Expected behavior: Clarifies that only revenue and freight are available — gross margin cannot be calculated. May offer revenue-minus-freight as a partial proxy with clear caveats.
    - Failure mode: Calculates (revenue - freight) / revenue and labels it "profit margin" without flagging the missing COGS

14. **How does our NPS compare to competitors?**
    - Why unanswerable: No competitor data exists in the dataset. Olist has review scores but no NPS survey data and no external benchmarks.
    - Expected behavior: States clearly that competitor benchmarks are not in the dataset. May offer internal review score trends as a related but distinct metric.
    - Failure mode: Invents industry benchmarks or treats review scores as equivalent to NPS without disclosure

---

## Demo Scenarios

Recommended question + dataset pairings for portfolio demos:

| Demo | Question | Why It Works |
|------|----------|--------------|
| Primary | "What is the monthly revenue trend and which product categories drive the most growth?" | Multi-step, clear output, visually compelling |
| Secondary | "What is the relationship between delivery time and customer review scores?" | Operational insight, regional segmentation, strong chart potential |
| Stress test | "Who are our best customers?" | Surfaces clarifying question behavior — good live demo moment |
