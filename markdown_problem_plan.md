# Markdown Metrics Update - Problem & Plan

> **Created:** 2025-12-01  
> **Meeting Date:** 2025-11-12  
> **Status:** 🟡 In Progress

---

## 🎯 Objective

Update the markdown metrics to capture **all markdowns** (not just digital/online), while maintaining backward compatibility and consolidating redundant queries.

---

## 📊 Current State vs Target State

### Metrics

| Metric | Current State | Target State |
|--------|---------------|--------------|
| Markdown | Only digital/online markdowns from `ONLINE_REDEEMING_HHS` | Split into two separate metrics |
| Digital Markdown | ❌ Not explicitly named | ✅ Keep existing (from redemptions query) |
| Total Markdown | ❌ Missing | ✅ NEW - from `TXN_FACTS` (includes all markdowns) |

### Data Sources

| Source Table | Current Usage | Target Usage |
|--------------|---------------|--------------|
| `ONLINE_REDEEMING_HHS` | All markdown calculations | Digital Markdown only |
| `TXN_FACTS` | Not used for markdown | Total Markdown source |

---

## 🔧 Technical Changes Required

### 1. Metric Workflow (`metric_workflow.ipynb`)

#### A. Add Total Markdown to Combined Transactions Query

**Location:** `combined_txns_query` (Cell 10)

**Formula:**
```sql
-- Total Markdown = Gross - Net Sales
-- Where Net Sales already accounts for markdowns
SUM(GROSS_AMT) - SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS TOTAL_MARKDOWN
```

**Validation:**
```
Revenue (GROSS_AMT) = Net Sales + Total Markdown
```

#### B. Query Consolidation (Future)

| Query | Action | Reason |
|-------|--------|--------|
| `combined_txns_query` | ✅ KEEP & EXTEND | Primary query for transaction metrics |
| `agp_query` | ⚠️ DEPRECATE | Has incorrect metrics; duplicate AGP with different numbers |
| `redemptions_query` | ✅ KEEP AS-IS | Becomes source for "Digital Markdown" |

**Problem with AGP Query:**
- Two AGP metrics exist with the same name but different values
- One from `combined_txns_query`, one from `agp_query`
- AGP query metrics were found to be incorrect
- Causes confusion and inconsistency in SAFE reports

**Solution:**
- Merge necessary AGP calculations into `combined_txns_query`
- Deprecate standalone `agp_query`
- Reduces maintenance burden and computation time

### 2. Safe Display / Safe Update

- [ ] Add `total_markdown` column to output tables
- [ ] Rename existing markdown to `digital_markdown`
- [ ] Ensure `ecom_sales` is included (already done per meeting)

---

## ✅ Validation & QA Plan

### Validation Checks

1. **Reconciliation Check:**
   ```
   Net Sales + Total Markdown ≈ Revenue (GROSS_AMT)
   ```

2. **Digital Offer ID Filtering:**
   - Verify PND/digital selection criteria in redemptions query
   - Ensure only digital offers are captured for Digital Markdown

3. **Cross-Experiment Validation:**
   - Run QA across 2-3 experiments
   - Compare totals in final tables

### Test Experiments
- [ ] Experiment 1: _________________
- [ ] Experiment 2: _________________
- [ ] Experiment 3: _________________

---

## 🔄 Backward Compatibility

### Requirements
- **DO NOT** remove existing columns
- **ADD** new columns alongside existing ones
- Legacy SAFE notebooks must continue to work

### Strategy
1. Add `total_markdown` as a new column (don't replace existing)
2. Add `digital_markdown` (can be alias or rename of existing)
3. Keep AGP query temporarily until consolidation is verified
4. Include `total_markdown` and `ecom_sales` in display tables to avoid rerunning old notebooks

---

## 📁 Files to Modify

| File | Changes | Priority |
|------|---------|----------|
| `metric_workflow.ipynb` | Add Total Markdown to combined_txns_query | 🔴 High |
| `metric_workflow.ipynb` | Consolidate AGP query (future) | 🟡 Medium |
| Safe Display notebook | Add total_markdown column | 🔴 High |
| Safe Update notebook | Include new columns | 🟡 Medium |

---

## 📅 Action Items

- [ ] Implement Total Markdown in `combined_txns_query`
- [ ] Retain Digital Markdown from redemptions as separate column
- [ ] Update Safe Display to include new columns
- [ ] Validate totals across 2-3 experiments
- [ ] Verify digital offer ID selection criteria (PND/digital)
- [ ] Document formulas and validation steps
- [ ] (Future) Consolidate AGP and combined transactions queries
- [ ] (Future) Define deprecation timeline for AGP query

---

## ⚠️ Open Questions

1. **Final query naming:** What should the consolidated query be called?
2. **Deprecation timeline:** When can we fully remove AGP query?
3. **Legacy support:** How long must old notebooks be supported?
4. **Threshold for discrepancies:** What variance is acceptable in QA validation?

---

## 📝 Meeting Notes Reference

**Participants:** Speaker 1, Speaker 2 (Basheesh), Speaker 3 (Suyash), Speaker 4 (Vishwas), Speaker 5

**Key Decisions:**
1. Use `combined_txns_query` for Total Markdown (not AGP query)
2. Keep redemptions query as-is for Digital Markdown
3. Add new columns without removing existing ones
4. Include both metrics in display tables

**Communication:**
- Questions: Ping Basheesh or Vishwas in group chat
- QA updates: Keep in separate QA group to avoid contamination

---

## 🔗 Related Resources

- Metric Workflow Notebook: `metric_workflow.ipynb`
- GitHub Repo: https://github.com/justsuyash/metric_workflow

