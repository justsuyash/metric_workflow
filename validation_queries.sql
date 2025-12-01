-- Validation Queries for Markdown Metrics
-- Goal: Determine if specific registers need to be filtered to match Total Markdown with Net Sales/Revenue

-- -----------------------------------------------------------------------------
-- QUERY 1: BASELINE (No Register Filters)
-- Calculate Totals from TXN_FACTS directly to see the "Raw" numbers
-- -----------------------------------------------------------------------------
SELECT
    'BASELINE_NO_FILTERS' AS SCENARIO,
    SUM(GROSS_AMT) AS TOTAL_GROSS_AMT,
    SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS TOTAL_NET_SALES,
    SUM(GROSS_AMT) - SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS TOTAL_MARKDOWN_CALC,
    SUM(MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS ALLOCATED_MKDNS -- Checking component parts
FROM `gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.txn_facts`
WHERE
    TXN_DTE >= DATE('2024-01-01') -- Replace with your test date range
    AND TXN_DTE < DATE('2024-01-08')
    AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
    AND MISC_ITEM_QTY = 0
    AND DEPOSIT_ITEM_QTY = 0;


-- -----------------------------------------------------------------------------
-- QUERY 2: FILTERED (With Standard Register Filters)
-- This mimics the logic in 'combined_txns_query'
-- -----------------------------------------------------------------------------
WITH filtered_txns AS (
    SELECT
        TXN_ID,
        TXN_DTE,
        SUM(GROSS_AMT) AS GROSS_AMT,
        SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS NET_SALES
    FROM `gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.txn_facts`
    WHERE
        TXN_DTE >= DATE('2024-01-01') -- Replace with your test date range
        AND TXN_DTE < DATE('2024-01-08')
        AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
        AND MISC_ITEM_QTY = 0
        AND DEPOSIT_ITEM_QTY = 0
    GROUP BY 1, 2
)
SELECT
    'FILTERED_REGISTERS' AS SCENARIO,
    SUM(tf.GROSS_AMT) AS TOTAL_GROSS_AMT,
    SUM(tf.NET_SALES) AS TOTAL_NET_SALES,
    SUM(tf.GROSS_AMT) - SUM(tf.NET_SALES) AS TOTAL_MARKDOWN_CALC
FROM filtered_txns tf
JOIN `gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.TXN_HDR_COMBINED` f
    ON tf.TXN_ID = f.TXN_ID AND tf.TXN_DTE = f.TXN_DTE
WHERE
    f.TXN_HDR_SRC_CD = 0
    -- Standard Register Filter List from Notebook
    AND f.REGISTER_NBR IN (
        99, 104, 173, 174, 999,
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        49, 50, 51, 52, 53, 54, 93, 94, 95, 96, 97, 98,
        116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
        151, 152, 153, 154, 175, 176, 177, 178, 179, 180,
        181, 182, 195, 196, 197, 198
    );


-- -----------------------------------------------------------------------------
-- QUERY 3: EXCLUDED REGISTERS IMPACT
-- See exactly what we are dropping by applying the filter
-- -----------------------------------------------------------------------------
WITH filtered_txns AS (
    SELECT
        TXN_ID,
        TXN_DTE,
        SUM(GROSS_AMT) AS GROSS_AMT,
        SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS NET_SALES
    FROM `gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.txn_facts`
    WHERE
        TXN_DTE >= DATE('2024-01-01') -- Replace with your test date range
        AND TXN_DTE < DATE('2024-01-08')
        AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
        AND MISC_ITEM_QTY = 0
        AND DEPOSIT_ITEM_QTY = 0
    GROUP BY 1, 2
)
SELECT
    f.REGISTER_NBR,
    COUNT(DISTINCT f.TXN_ID) AS TXN_COUNT,
    SUM(tf.GROSS_AMT) AS GROSS_AMT,
    SUM(tf.GROSS_AMT) - SUM(tf.NET_SALES) AS TOTAL_MARKDOWN
FROM filtered_txns tf
JOIN `gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.TXN_HDR_COMBINED` f
    ON tf.TXN_ID = f.TXN_ID AND tf.TXN_DTE = f.TXN_DTE
WHERE
    f.TXN_HDR_SRC_CD = 0
    AND f.REGISTER_NBR NOT IN (
        99, 104, 173, 174, 999,
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        49, 50, 51, 52, 53, 54, 93, 94, 95, 96, 97, 98,
        116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
        151, 152, 153, 154, 175, 176, 177, 178, 179, 180,
        181, 182, 195, 196, 197, 198
    )
GROUP BY 1
ORDER BY 3 DESC;

