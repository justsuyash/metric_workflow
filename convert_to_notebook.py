#!/usr/bin/env python3
"""
Convert safe_latest.py to safe.ipynb with TOTAL_MARKDOWN and DIGITAL_MARKDOWN modifications
"""
import json
import re

def read_file(filepath):
    with open(filepath, 'r') as f:
        return f.read()

def write_notebook(cells, filepath):
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    with open(filepath, 'w') as f:
        json.dump(notebook, f, indent=2)

def create_code_cell(source):
    lines = source.rstrip('\n').split('\n')
    source_list = [line + '\n' for line in lines[:-1]]
    if lines:
        source_list.append(lines[-1])
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source_list
    }

def create_markdown_cell(source):
    lines = source.rstrip('\n').split('\n')
    source_list = [line + '\n' for line in lines[:-1]]
    if lines:
        source_list.append(lines[-1])
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source_list
    }

def convert_py_to_notebook(py_content):
    """Split Python file into notebook cells based on %md markers"""
    cells = []
    parts = re.split(r'\n%md\n', py_content)
    
    for i, part in enumerate(parts):
        if i == 0:
            if part.strip():
                cells.append(create_code_cell(part.strip()))
        else:
            lines = part.split('\n')
            md_lines = []
            code_start = 0
            
            for j, line in enumerate(lines):
                if line.startswith('#') and not line.startswith('##---'):
                    md_lines.append(line)
                    code_start = j + 1
                elif line.strip() == '':
                    if md_lines:
                        code_start = j + 1
                    continue
                else:
                    break
            
            if md_lines:
                cells.append(create_markdown_cell('\n'.join(md_lines)))
            
            remaining_code = '\n'.join(lines[code_start:]).strip()
            if remaining_code:
                cells.append(create_code_cell(remaining_code))
    
    return cells

def add_markdown_modifications(py_content):
    """Add TOTAL_MARKDOWN metric and rename existing to DIGITAL_MARKDOWN"""
    
    # 1. Add test table variable after standard table definitions
    py_content = py_content.replace(
        'combined_txn_table = "db_work.EXP_COE_COMBINED_TXNS_GCP"',
        '''combined_txn_table = "db_work.EXP_COE_COMBINED_TXNS_GCP"
  # TEST: Uncomment below to use markdown_test table for TOTAL_MARKDOWN comparison
  # combined_txn_table = "db_work.EXP_COE_COMBINED_TXNS_GCP_markdown_test"'''
    )
    
    # 2. Add TOTAL_MARKDOWN to TXNS CTE in ecomm_txns_agg query
    py_content = py_content.replace(
        'COALESCE(SUM(t.NET_SALES),0) AS TOT_NET_SALES\nFROM EXPOSURE_BASE as e\nLEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = \'ECOMM\'',
        '''COALESCE(SUM(t.NET_SALES),0) AS TOT_NET_SALES,
     COALESCE(SUM(t.TOTAL_MARKDOWN),0) AS TOT_TOTAL_MARKDOWN
FROM EXPOSURE_BASE as e
LEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = 'ECOMM\''''
    )
    
    # 3. Add TOTAL_MARKDOWN to TXNS CTE in store_txns_agg query
    py_content = py_content.replace(
        "COALESCE(SUM(t.NET_SALES),0) AS TOT_NET_SALES\nFROM EXPOSURE_BASE as e\nLEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = 'STORE'",
        '''COALESCE(SUM(t.NET_SALES),0) AS TOT_NET_SALES,
     COALESCE(SUM(t.TOTAL_MARKDOWN),0) AS TOT_TOTAL_MARKDOWN
FROM EXPOSURE_BASE as e
LEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = 'STORE\''''
    )
    
    # 4. Add TOTAL_MARKDOWN aggregation to TXN_AGG for ECOMM (after NET_SALES section)
    py_content = py_content.replace(
        '''--- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS ECOMM_AOV''',
        '''--- TOTAL MARKDOWN (NEW - from combined_txns, captures ALL markdowns)
       , SUM(TOT_TOTAL_MARKDOWN) AS ECOMM_TOTAL_MARKDOWN_SUM
       , AVG(TOT_TOTAL_MARKDOWN) AS ECOMM_TOTAL_MARKDOWN_MEAN
       , STDDEV(TOT_TOTAL_MARKDOWN) AS ECOMM_TOTAL_MARKDOWN_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS ECOMM_AOV'''
    )
    
    # 5. Add TOTAL_MARKDOWN aggregation to TXN_AGG for STORE
    py_content = py_content.replace(
        '''--- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS STORE_AOV''',
        '''--- TOTAL MARKDOWN (NEW - from combined_txns, captures ALL markdowns)
       , SUM(TOT_TOTAL_MARKDOWN) AS STORE_TOTAL_MARKDOWN_SUM
       , AVG(TOT_TOTAL_MARKDOWN) AS STORE_TOTAL_MARKDOWN_MEAN
       , STDDEV(TOT_TOTAL_MARKDOWN) AS STORE_TOTAL_MARKDOWN_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS STORE_AOV'''
    )
    
    # 6. Update the markdown display to show both DIGITAL and TOTAL markdown
    py_content = py_content.replace(
        "display(markdown_for_display[['VARIANT_ID','MKDN_TOTAL','ECOMM_MKDN_TOTAL','STORE_MKDN_TOTAL','pd_MKDN_TOTAL', 'gr_MKDN_TOTAL', 'mf_MKDN_TOTAL', 'spd_MKDN_TOTAL', 'pzn_MKDN_TOTAL', 'sc_MKDN_TOTAL']])",
        '''# Rename columns for clarity: existing MKDN = Digital Markdown (from redemptions)
markdown_for_display = markdown_for_display.rename(columns={
    'MKDN_TOTAL': 'DIGITAL_MKDN_TOTAL',
    'ECOMM_MKDN_TOTAL': 'ECOMM_DIGITAL_MKDN',
    'STORE_MKDN_TOTAL': 'STORE_DIGITAL_MKDN'
})

print("=== MARKDOWN COMPARISON ===")
print("DIGITAL_MKDN = From redemptions table (digital offers only)")
print("TOTAL_MARKDOWN = From combined_txns (ALL markdowns including non-digital)")
print("")

display(markdown_for_display[['VARIANT_ID','DIGITAL_MKDN_TOTAL','ECOMM_DIGITAL_MKDN','STORE_DIGITAL_MKDN','pd_MKDN_TOTAL', 'gr_MKDN_TOTAL', 'mf_MKDN_TOTAL', 'spd_MKDN_TOTAL', 'pzn_MKDN_TOTAL', 'sc_MKDN_TOTAL']])'''
    )
    
    # 7. Add TOTAL_MARKDOWN t-test after the existing markdown t-tests (after pzn_mkdn section)
    py_content = py_content.replace(
        "except:\n  print('No PZN deals')\ntry:\n  pzn_mkdn[1].show()\nexcept:\n  print('No PZN deals')\n%md\n# Ecomm Metrics",
        '''except:
  print('No PZN deals')
try:
  pzn_mkdn[1].show()
except:
  print('No PZN deals')

# === TOTAL MARKDOWN T-TEST (NEW) ===
# This tests the TOTAL_MARKDOWN from combined_txns table
# which includes ALL markdowns (digital + non-digital)
print("\\n=== TOTAL MARKDOWN (from combined_txns - ALL markdowns) ===")
try:
    total_markdown_test = hypothesis_test_compare_means(
        df = ecomm_agg_df,
        variant_column_name = "VARIANT_ID",
        control_variant_name = control_variant_nm,
        mean_column_name = "ECOMM_TOTAL_MARKDOWN_MEAN",
        std_column_name = "ECOMM_TOTAL_MARKDOWN_SD",
        n_column_name = "VISITORS",
        metric_type = "mean",
        pooled = False,
        one_tailed = False,
        ci_level = 1-SIGNIFICANCE, 
        positive_good = True,
        rounding = 3
    )
    display(total_markdown_test[0])
    total_markdown_test[1].show()
except Exception as e:
    print(f"TOTAL_MARKDOWN test skipped - column may not exist yet: {e}")
    print("Run markdown_fix.ipynb first to create the test table with TOTAL_MARKDOWN column")
%md
# Ecomm Metrics'''
    )
    
    return py_content

def main():
    print("Reading safe_latest.py...")
    py_content = read_file('/Users/justsuyash/Documents/GitHub/metric_workflow/safe_latest.py')
    print(f"Read {len(py_content)} characters")
    
    print("Applying TOTAL_MARKDOWN modifications...")
    py_content = add_markdown_modifications(py_content)
    
    print("Converting to notebook cells...")
    cells = convert_py_to_notebook(py_content)
    print(f"Created {len(cells)} cells")
    
    header_cell = create_markdown_cell("""# SAFE Analysis Notebook (with TOTAL_MARKDOWN)

This notebook includes **two markdown metrics** for comparison:

| Metric | Source | Description |
|--------|--------|-------------|
| **DIGITAL_MKDN** | `redemptions_table` | Only digital/online redemption markdowns |
| **TOTAL_MARKDOWN** | `combined_txns_table` | ALL markdowns (digital + in-store) |

## How to Test

1. **Run `markdown_fix.ipynb` first** to create the test table with TOTAL_MARKDOWN
2. Uncomment the test table line in Cell 2 (table definitions)
3. Run this notebook and compare DIGITAL_MKDN vs TOTAL_MARKDOWN

## Key Changes
- Existing `MKDN_TOTAL` renamed to `DIGITAL_MKDN_TOTAL` for clarity
- New `TOTAL_MARKDOWN` calculated as (REVENUE - NET_SALES)
- Added t-test for TOTAL_MARKDOWN in the Markdown section""")
    
    cells.insert(0, header_cell)
    
    print("Writing safe.ipynb...")
    write_notebook(cells, '/Users/justsuyash/Documents/GitHub/metric_workflow/safe.ipynb')
    print("SUCCESS! Notebook created: safe.ipynb")
    print(f"Total cells: {len(cells)}")

if __name__ == "__main__":
    main()
