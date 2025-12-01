# Why We Are Updating the Markdown Logic
*(A Simple Explanation with Data)*

## 🍋 The "Lemonade Stand" Explanation

Imagine you run a **Lemonade Stand** and a **Gas Station**.

*   **Lemonade Sales:** $100
*   **Gas Sales:** $50

You are writing a report **only about the Lemonade Stand**.

1.  **Correct Way:**
    *   Report Sales: $100 (Lemonade only)
    *   Report Discounts: $10 (Lemonade coupons only)
    *   **Math:** You sold $110 worth of lemonade for $100. ($110 - $10 = $100). **It balances.** ✅

2.  **The Problem We Found (The "Raw" Markdown):**
    *   Report Sales: $100 (Lemonade only - Correct)
    *   Report Discounts: **$15** (Includes $10 Lemonade coupons + $5 Gas coupons)
    *   **Math:** You claimed you sold $110 worth of lemonade... but subtracted $15 discounts... which leaves $95.
    *   **Mismatch:** You reported $100 in sales, but your discounts imply you only made $95. **The numbers don't add up.** ❌

**Conclusion:** We must exclude the "Gas Station" discounts (Fuel/Pharmacy registers) from our "Lemonade Stand" report so the math balances.

---

## 📊 The Real Data (From Our Validation)

We ran a test to compare the "Raw" numbers vs. the "Filtered" numbers using our actual data.

### Scenario A: The "Raw" Numbers (Includes Everything)
*   **Gross Sales:** $1.59 Billion
*   **Markdown:** **$220 Million**
*   *Includes Fuel, Pharmacy, and other excluded registers.*

### Scenario B: The "Reported" Numbers (What we currently show in SAFE)
*   **Gross Sales:** $1.35 Billion
*   **Markdown:** **$214 Million**
*   *Filters OUT Fuel, Pharmacy, etc.*

### 🚩 The Discrepancy
If we simply pulled "Total Markdown" from the database without filters, we would be reporting **$220 Million** in markdowns against a sales base of only **$1.35 Billion**.

*   **Difference:** **$5.7 Million** in markdowns come from registers we explicitly exclude (like Register 23, 47, and Fuel).
*   **Impact:** If we use the raw number, our reported Net Sales + Markdowns will be **$5.7 Million HIGHER** than our reported Gross Sales. The finance team would flag this immediately as an error.

---

## ✅ The Solution

We are moving the **Total Markdown** calculation *inside* our main Sales Query (`combined_txns_query`).

**Why?**
Because the Sales Query already has the strict list of "Allowed Registers" (Registers 1-20, E-com, etc.). By calculating markdown in the same place, it automatically applies the same rules.

**The Result:**
*   **Sales:** Correctly Filtered ($1.35B)
*   **Markdown:** Correctly Filtered ($214M)
*   **Math Check:** Net Sales + Markdown = Gross Sales. **Perfect Balance.** ✅

---

## 🚀 Summary of Metrics

We will now provide two clear metrics:

1.  **Total Markdown (New):**
    *   Includes **ALL** discounts (Store + Digital) for the reported transactions.
    *   Matches the P&L (Profit & Loss) math.

2.  **Digital Markdown (Existing):**
    *   Includes **ONLY** discounts from digital coupons/offers.
    *   Helps us measure the specific impact of our digital programs.

