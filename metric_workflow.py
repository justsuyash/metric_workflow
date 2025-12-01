%run /Workspace/Projects/Experimentation/aaml-experimentation-coe/exp_coe_utils
##---------------------------------------------------------------------------------------------------------------------------##
## System & Notebook Setup                                                                                                   

## System Settings
pd.set_option('display.max_colwidth', 0)

## Notebook Input Widgets
# dbutils.widgets.text("01. Start Date", defaultValue="")
# dbutils.widgets.text("02. End Date", defaultValue="")
# dbutils.widgets.text(name = "experiment_id", label="03. Experiment ID", defaultValue="")
# dbutils.widgets.dropdown(name="metric_selection", defaultValue="SAFE", choices=["SAFE", "Power", "Attributes"], label = "04. Metric Selection")
# dbutils.widgets.removeAll()



##---------------------------------------------------------------------------------------------------------------------------##
start_date = str(getArgument("01. Start Date"))
end_date = str(getArgument("02. End Date"))
if start_date:
  start_date = "'" + str(getArgument("01. Start Date")) + "'"
  start_date_tz = f''' DATETIME({start_date} , 'America/Boise') '''
if end_date:
  end_date = "'" + str(getArgument("02. End Date")) + "'"
  end_date_tz = f''' DATETIME({end_date} , 'America/Boise') '''
exp_id = getArgument("03. Experiment ID")
metric_selection = getArgument("04. Metric Selection")


## Must provide ALL of:  (1) start date, (2) end date and (3) experiment id when doing a custom SAFE or Power runs.
if exp_id != "":
  if (start_date == "") or (end_date == ""):
    dbutils.notebook.exit("PLEASE FIX INPUTS:  If providing a custom experiment ID for a SAFE or Power metric run, please specify start and end dates. Otherwise, use the standard metric tables in db_work.")

## Attribute runs do not require any other inputs.
if (metric_selection == "Attributes") and (exp_id != ""):
  dbutils.notebook.exit("PLEASE FIX INPUTS:  Attribute runs do not depend on experiment ID, start date or end dates.  Please do not input these values when doing Attribute runs.")

## Check if only one of start date or end date is provided.
if (start_date != "" and end_date == "") or (start_date == "" and end_date != ""):
  dbutils.notebook.exit("PLEASE FIX INPUTS:  Must provide inputs for both start and end dates, or leave both blank.")

## Check if start and end date provided without EXP_ID
elif (start_date != "") and (end_date != "") and (exp_id == ""):
  dbutils.notebook.exit("PLEASE FIX INPUTS:  Must provide inputs for both start and end dates, and Experiment ID .")

## If no start/end dates are selected.  Use default values.
elif start_date == "" and end_date == "":
  print(f"""No Start and End Dates Provided, will use last 90 Days for Experimentation Metrics or 365 Days for Power Metrics.""")
  
  if metric_selection == "Power":
    start_date = "CURRENT_DATE()-365"
    start_date_tz = f"""CURRENT_DATE("America/Boise")-365"""
  elif metric_selection == "SAFE":
    start_date = "CURRENT_DATE()-90"
    start_date_tz = f"""CURRENT_DATE("America/Boise")-90"""
  elif metric_selection == "Attributes":
    start_date = "CURRENT_DATE()-90"
    start_date_tz = f"""CURRENT_DATE("America/Boise")-90"""
  
  end_date = "CURRENT_DATE()"
  end_date_tz = f"""CURRENT_DATE("America/Boise")"""

print("Inputs For Notebook:")
print(f"""Start Date:  {start_date}""")
print(f"""End Date:  {end_date}""")
print(f"""Start Date TimeZone:  {start_date_tz}""")
print(f"""End Date TimeZone:  {end_date_tz}""")
print(f"""Experiment ID:  {exp_id}""")
print(f"""Metrics that will be executed:  {metric_selection}""")
CLICK_STREAM_VISIT_VIEW = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CLICK_STREAM_VISIT_VIEW'
CLICK_STREAM_ORDER_JOURNEY = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CLICK_STREAM_ORDER_JOURNEY'
CLICK_HIT_DATA = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CLICK_HIT_DATA'
EB_HH_STORE_WKLY_AGP_ALLDIV_VIEW = 'gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.EB_HH_STORE_WKLY_AGP_ALLDIV_VIEW'
D0_FISCAL_WEEK = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_acct.D0_FISCAL_WEEK'
TXN_HDR_COMBINED = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.TXN_HDR_COMBINED'
LU_DAY_MERGE = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_edw.LU_DAY_MERGE'
SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD'
LOY_CLIPS = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_clips'
OFFER_BANK = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_mrch.OFFER_BANK'
ONLINE_REDEEMING_HHS = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.c360_online_redeeming_hhs'
#ONLINE_REDEEMING_HHS = 'gcp-abs-udco-bq-prod-prj-01.udco_ds_cust_analytics.C360_ONLINE_REDEEMING_HHS'
LOY_REWARDS_REDEEMED = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_REWARDS_REDEEMED'
ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT'
C360_CUSTOMER_PROFILE = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_CUSTOMER_PROFILE'
ESA_HOLISTIC_SCORECARD_ORDER_METRICS = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.ESA_HOLISTIC_SCORECARD_ORDER_METRICS'
ESA_HOLISTIC_SCORECARD_BASKET_HEALTH = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.ESA_HOLISTIC_SCORECARD_BASKET_HEALTH'
ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT'
TXN_FACTS = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.txn_facts'
D1_UPC = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_mrch.d1_upc'
D1_RETAIL_STORE = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_locn.D1_RETAIL_STORE'
CUSTOMER_DIGITAL_CONTACT = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CUSTOMER_DIGITAL_CONTACT'
CUSTOMER_PHONE_FAX_CONTACT = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CUSTOMER_PHONE_FAX_CONTACT'
C360_STORE = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_STORE'
STORE_UPC_AGP = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.STORE_UPC_AGP' #'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_edw.store_upc_agp'
GW_REG99_TXNS_2020 = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_edw.GW_REG99_TXNS_2020'
PARTNER_ORDER_STORE_TENDER = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_edw.PARTNER_ORDER_STORE_TENDER'
EB_ECOMM_FEE = 'gcp-abs-aamp-wmfs-prod-prj-01.aamp_ds_pz_wkg.EB_ECOMM_FEE'
SFMC_PUSH_POPULATION = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.SFMC_PUSH_POPULATION'
LEAP = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.LEAP'
SMS = 'gcp-abs-udco-lobz-prod-prj-01.udco_ds_loyalty.SMS_Population'
FACTS_SEGMENT_WEEK = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_mrch.FACTS_SEGMENT_WEEK'
CA_MA_ATTITUDINAL_SEGMENTATION_MASTER = 'gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CA_MA_ATTITUDINAL_SEGMENTATION_MASTER'
JAA_HH_DIV_PRIMARY_STORE = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.JAA_HH_DIV_PRIMARY_STORE'
EXP_COE_3P_HHS_LIST = 'gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.EXP_COE_3P_HHS_LIST'
SMS_POPULATION = 'gcp-abs-udco-lobz-prod-prj-01.udco_ds_loyalty.SMS_Population'
if exp_id == "":

  ## SAFE Metric Output tables
  VISIT_ORDER = "db_work.EXP_COE_VISIT_ORDER_GCP"
  CART_COUPON = "db_work.EXP_COE_CART_COUPON_GCP"
  MARGIN = "db_work.EXP_COE_MARGIN_GCP"
  AGP = "db_work.EXP_COE_AGP_GCP"
  COMBINED_TXNS = "db_work.EXP_COE_COMBINED_TXNS_GCP"
  CLIPS = "db_work.EXP_COE_CLIPS_GCP"
  REDEMPTIONS = "db_work.EXP_COE_REDEMPTIONS_GCP"
  GAS_TXNS = "db_work.EXP_COE_GAS_TXNS_GCP"
  BASKET_TIME_TXNS = "db_work.EXP_COE_BASKET_TIME_TXNS_GCP"
  BNC = "db_work.EXP_COE_BNC_GCP"
  BASKET_HEALTH = "db_work.EXP_COE_BASKET_HEALTH_GCP"
  CATEGORY_TXNS = "db_work.EXP_COE_CATEGORY_TXNS_GCP"
  EXP_COE_ACC_HEALTH = "db_work.EXP_COE_ACC_HEALTH_GCP"
  EXP_COE_3P_HHS_LIST_DB_WORK = "db_work.EXP_COE_3P_HHS_LIST_GCP"

  ## Power Metric Output tables
  EXPOSURE_VISIT_FOR_POWER = "db_work.EXP_COE_EXPOSURE_VISIT_FOR_POWER_GCP"
  TXN_FOR_POWER = "db_work.EXP_COE_TXN_FOR_POWER_GCP"
  PUSH_OPT_IN = "db_work.EXP_COE_PUSH_OPT_IN_GCP"
  EMAIL_OPT_IN = "db_work.EXP_COE_EMAIL_OPT_IN_GCP"
  SMS_OPT_IN = "db_work.EXP_COE_SMS_OPT_IN_GCP"
  REWARDS_ENGAGED = "db_work.EXP_COE_REWARDS_ENGAGED_GCP"
  FACT_SEGMENTS = "db_work.EXP_COE_FACT_SEGMENTS_GCP"
  MYNEEDS_SEGMENTS = "db_work.EXP_COE_MYNEEDS_SEGMENTS_GCP"
  PRIMARY_PLATFORM = "db_work.EXP_COE_PRIMARY_PLATFORM_GCP"
  DIVISION_MAPPING = "db_work.EXP_COE_DIVISION_MAPPING_GCP"

else:
  ## Custom SAFE Metric Output tables, when an experiment ID is provided
  VISIT_ORDER = f"""db_work.visit_order_sp_{exp_id}"""
  CART_COUPON = f"""db_work.cart_coupon_sp_{exp_id}"""
  MARGIN = f"""db_work.margin_sp_{exp_id}"""
  AGP = f"""db_work.agp_sp_{exp_id}"""
  COMBINED_TXNS = f"""db_work.combined_txns_sp_r1_{exp_id}"""
  CLIPS = f"""db_work.clips_sp_{exp_id}"""
  REDEMPTIONS = f"""db_work.redemptions_sp_r1_{exp_id}"""
  GAS_TXNS = f"""db_work.gas_txns_sp_{exp_id}"""
  BASKET_TIME_TXNS = f"""db_work.basket_time_sp_{exp_id}"""
  BNC = f"""db_work.bnc_sp_{exp_id}"""
  BASKET_HEALTH = f"""db_work.basket_health_sp_{exp_id}"""
  CATEGORY_TXNS = f"""db_work.category_sp_{exp_id}"""
  EXP_COE_ACC_HEALTH = f"""db_work.account_health_sp_{exp_id}"""

  ## Power Metric Output tables
  EXPOSURE_VISIT_FOR_POWER = f'''db_work.exposure_visit_for_power_sp_{exp_id}'''
  TXN_FOR_POWER = f'''db_work.txn_for_power_{exp_id}'''
  PUSH_OPT_IN = f'''db_work.push_opt_in_{exp_id}'''
  EMAIL_OPT_IN = f'''db_work.email_opt_in_{exp_id}'''
  SMS_OPT_IN = f'''db_work.sms_opt_in_{exp_id}'''
  REWARDS_ENGAGED = f'''db_work.rewards_engaged_{exp_id}'''
  FACT_SEGMENTS = f'''db_work.fact_segments_{exp_id}'''
  MYNEEDS_SEGMENTS = f'''db_work.myneeds_segments_{exp_id}'''
  PRIMARY_PLATFORM = f'''db_work.primary_platform_{exp_id}'''
  DIVISION_MAPPING = f'''db_work.division_mapping_{exp_id}'''

# print(VISIT_ORDER)
# print(CART_COUPON)
# print(MARGIN)
# print(AGP)
# print(COMBINED_TXNS)
# print(CLIPS)
# print(REDEMPTIONS)
# print(GAS_TXNS)
# print(BASKET_TIME_TXNS)
# print(BNC)
# print(BASKET_HEALTH)
# print(CATEGORY_TXNS)
# print(EXP_COE_ACC_HEALTH)

# print(EXPOSURE_VISIT_FOR_POWER)
# print(TXN_FOR_POWER)
# print(PUSH_OPT_IN)
# print(EMAIL_OPT_IN)
# print(FACT_SEGMENTS)
# print(MYNEEDS_SEGMENTS)
# print(PRIMARY_PLATFORM)
# print(DIVISION_MAPPING)
%md
--------------------------------------------
# SAFE METRICS
1. If Start and End dates are not provided, then defaults to the past 90 days. 
2. Enter a start and end date and provide an Experiment ID to save for a custom run, that saves data to a custom location.  MUST provide start and end dates when using a custom Experiment ID.
visit_order_query = (
  f"""
    SELECT 
      v.VISIT_ID
      , v.VISITOR_ID AS ID
      , SAFE_CAST(v.RETAIL_CUSTOMER_HHID_TXT AS INT) AS HOUSEHOLD_ID
      , TIMESTAMP(v.visit_start_ts) AS VISIT_START_TS
      , TIMESTAMP(v.visit_end_ts) AS VISIT_END_TS
      , SAFE_CAST(COUNT(DISTINCT o.ORDER_ID) AS INT) AS NUM_ORDERS
      , SAFE_CAST(SUM(COALESCE(o.GROSS_REVENUE_AMT,0)) AS NUMERIC) AS TOT_REVENUE
      , SAFE_CAST(SUM(COALESCE(o.GROSS_UNIT_CNT,0)) AS INT) AS NUM_UNITS
    FROM 
      {CLICK_STREAM_VISIT_VIEW} AS v
    LEFT JOIN 
      {CLICK_STREAM_ORDER_JOURNEY} AS o
    ON v.VISIT_ID = o.VISIT_ID
      WHERE 1=1
        AND (
              ( DATE(TIMESTAMP(visit_start_ts)) >= ({start_date}) AND DATE(TIMESTAMP(visit_start_ts)) < {end_date} )
                OR 
              ( DATE(TIMESTAMP(visit_end_ts)) >= ({start_date}) AND DATE(TIMESTAMP(visit_end_ts)) < {end_date} )
            )
    GROUP BY 1,2,3,4,5 
  """
)

if metric_selection == "SAFE":
  print(visit_order_query)
  visit_order_sp = bc.read_gcp_table(visit_order_query)
  visit_order_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(VISIT_ORDER)

cart_coupon_query = (
  f"""
    SELECT
      DATE_TIME AS DTE
      , CONCAT(POST_VISID_HIGH, POST_VISID_LOW) AS ID
      , SAFE_CAST(POST_EVAR47 as INT) as HOUSEHOLD_ID
      , CASE
          WHEN CONCAT(',', post_event_list, ',') LIKE '%,12,%' THEN 'CART_ADD'
          WHEN CONCAT(',', post_event_list, ',') LIKE '%,20305,%' THEN 'COUPON_CLIP'
          WHEN CONCAT(',', post_event_list, ',') LIKE '%,200,%' THEN 'SEARCH'
          ELSE NULL
        END AS EVENT_TYPE
      FROM {CLICK_HIT_DATA}                                        
      WHERE SAFE_CAST(exclude_hit AS INT) = 0
        AND SAFE_CAST(hit_source AS INT) NOT IN (5,7,8,9)
            AND (CONCAT(',', post_event_list, ',') LIKE '%,12,%' OR CONCAT(',', post_event_list, ',') LIKE '%,20305,%' OR CONCAT(',', post_event_list, ',') LIKE '%,200,%')
            AND ( DATE(DATE_TIME) >= ({start_date}) AND DATE(DATE_TIME) < {end_date} )
      """
      )

if metric_selection == "SAFE":
  print(cart_coupon_query)
  cart_coupon_sp = bc.read_gcp_table(cart_coupon_query)
  cart_coupon_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(CART_COUPON)
margin_query = (
  f"""
    SELECT 
        SAFE_CAST(a.HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID
        , a.WEEK_ID
        , fw.FISCAL_WEEK_START_DT
        , fw.FISCAL_WEEK_END_DT
        , a.AGP_VAL_WITHOUT_INSTA_ADJ
    FROM {EB_HH_STORE_WKLY_AGP_ALLDIV_VIEW} AS a
    LEFT JOIN {D0_FISCAL_WEEK} AS fw
        ON a.WEEK_ID = fw.FISCAL_WEEK_ID
    WHERE (fw.FISCAL_WEEK_START_DT >= DATE_TRUNC(DATE({start_date})+3, WEEK) AND fw.FISCAL_WEEK_START_DT < DATE_TRUNC(DATE({end_date}),WEEK)) 
  """
  )

if metric_selection == "SAFE":
  print(margin_query)
  margin_sp = bc.read_gcp_table(margin_query)
  margin_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(MARGIN)
agp_query = (
  f"""
 WITH
 AGP AS(
 SELECT 
 STR.RETAIL_STORE_FACILITY_NBR AS STORE_ID
      ,AGP.UPC_NBR AS UPC_ID
      ,AGP.TRANSACTION_DT AS DAY_DT
      ,AGP.ITEM_QTY AS SUM_ITEM_QTY
      ,AGP.NET_AMT AS SUM_NET_SALES_AMT
      ,AGP.AGP_UNIT_AMT AS AGP_UNIT_AMT
      ,AGP.COST_OF_GOODS_AMT AS COST_OF_GOODS_AMT
      ,UPC.RETAIL_DEPARTMENT_ID AS DEPARTMENT_ID
 , CASE
 WHEN AGP.ITEM_QTY  = 0 OR AGP.ITEM_QTY  IS NULL THEN 0
 ELSE CAST(AGP.COST_OF_GOODS_AMT AS FLOAT64)/CAST(AGP.ITEM_QTY AS FLOAT64)
 END AS UPC_COGS_PER_UNIT_AMT
 , CASE
 WHEN AGP.ITEM_QTY = 0 OR AGP.ITEM_QTY IS NULL THEN 0
 ELSE CAST(AGP.NET_AMT AS FLOAT64)/CAST(AGP.ITEM_QTY AS FLOAT64)
 END AS UPC_AIV
 , CASE
 WHEN AGP.ITEM_QTY = 0 OR AGP.ITEM_QTY IS NULL THEN 0
 ELSE CAST(AGP.AGP_EXTENDED_AMT AS FLOAT64)/CAST(AGP.ITEM_QTY AS FLOAT64)
 END AS UPC_AGP_EXT_UNIT_AMT
 , CASE
 WHEN AGP.ITEM_QTY = 0 OR AGP.ITEM_QTY IS NULL THEN 0
 ELSE CAST(AGP.RETAIL_LATE_FLAT_ALLOWANCE_AMT AS FLOAT64)/CAST(AGP.ITEM_QTY AS FLOAT64)
 END AS UPC_RTL_LATE_FLAT_ALLOW_PER_UNIT_AMT
 , CASE
 WHEN AGP.ITEM_QTY = 0 OR AGP.ITEM_QTY IS NULL THEN 0
 ELSE CAST(AGP.RETAIL_NEW_ITEM_ALLOWANCE_AMT AS FLOAT64)/CAST(AGP.ITEM_QTY AS FLOAT64)
 END AS UPC_NEW_ITEM_ALLOW_PER_UNIT_AMT

 FROM {STORE_UPC_AGP} AS AGP ----table impacted
 JOIN {D1_UPC} AS UPC
 ON UPC.UPC_NBR = AGP.UPC_NBR
 AND UPC.SAFEWAY_UPC_IND=TRUE
 AND UPC.CORPORATION_ID = '001'

 INNER JOIN {D1_RETAIL_STORE} AS STR
 ON AGP.FACILITY_INTEGRATION_ID = STR.FACILITY_INTEGRATION_ID
 AND STR.DW_LOGICAL_DELETE_IND=FALSE

 WHERE UPC.SMIC_GROUP_ID < 99
 AND UPC.RETAIL_DEPARTMENT_ID NOT IN ('339')
 AND (AGP.TRANSACTION_DT >= ({start_date}) AND AGP.TRANSACTION_DT < {end_date})
 ),

 ------------ T1 (TXN info at the UPC level---------------
 T1 AS (
 SELECT
 CARD.HOUSEHOLD_ID
 , TXN.UPC_ID
 , TXN.STORE_ID
 , TXN.TXN_DTE
 , UPC.RETAIL_DEPARTMENT_ID AS DEPARTMENT_ID
 , UPC.SMIC_CATEGORY_ID AS CATEGORY_ID
 , UPC.SMIC_GROUP_ID AS GROUP_ID
 , TXN.TXN_ID
 , COALESCE(SUM(TXN.NET_AMT+TXN.MKDN_WOD_ALLOC_AMT+TXN.MKDN_POD_ALLOC_AMT),0) AS NET_SALES
 , COALESCE(SUM(TXN.ITEM_QTY),0) AS ITEM_QTY
 FROM {TXN_FACTS} AS TXN
 INNER JOIN {D1_UPC} AS UPC
 ON SAFE_CAST(TXN.UPC_ID AS BIGNUMERIC) = SAFE_CAST(UPC.UPC_NBR AS BIGNUMERIC)
 AND UPC.SAFEWAY_UPC_IND=TRUE
 INNER JOIN {D1_RETAIL_STORE} AS STR
 ON SAFE_CAST(TXN.STORE_ID AS BIGNUMERIC) = SAFE_CAST(STR.RETAIL_STORE_FACILITY_NBR AS BIGNUMERIC)
 AND STR.DW_LOGICAL_DELETE_IND=FALSE
 INNER JOIN (SELECT DISTINCT HOUSEHOLD_ID, LOYALTY_PROGRAM_CARD_NBR FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}) AS CARD
 ON SAFE_CAST(TXN.CARD_NBR AS BIGNUMERIC) = SAFE_CAST(CARD.LOYALTY_PROGRAM_CARD_NBR AS BIGNUMERIC)
 WHERE 1=1
 AND (TXN.TXN_DTE >= ({start_date}) AND TXN.TXN_DTE < ({end_date}))
 AND UPC.CORPORATION_ID = '001'
 AND TXN.REV_DTL_SUBTYPE_ID IN (0,6,7)
 AND TXN.DEPOSIT_ITEM_QTY = 0
 AND SAFE_CAST(CARD.HOUSEHOLD_ID AS BIGNUMERIC) > 0
 AND UPC.RETAIL_DEPARTMENT_ID NOT IN ('339')
 AND UPC.SMIC_GROUP_ID < 99
 GROUP BY all
 ),

 T2 AS (
 SELECT T1.*
 , CAST((ITEM_QTY * (UPC_AGP_EXT_UNIT_AMT - UPC_RTL_LATE_FLAT_ALLOW_PER_UNIT_AMT - UPC_NEW_ITEM_ALLOW_PER_UNIT_AMT)) AS FLOAT64) AS AGP_TOT
 FROM T1
 LEFT JOIN AGP
 ON T1.TXN_DTE = AGP.DAY_DT
 AND T1.UPC_ID = AGP.UPC_ID
 AND SAFE_CAST(T1.STORE_ID AS BIGNUMERIC) = SAFE_CAST(AGP.STORE_ID AS BIGNUMERIC)
 )

 SELECT
 HOUSEHOLD_ID
    , TXN_DTE
 , SUM(NET_SALES) AS NET_SALES
 , SUM(ITEM_QTY) AS ITEM_QTY
 , COALESCE(COUNT(DISTINCT T2.TXN_ID),0) AS TXNS
 , SUM(AGP_TOT ) AS AGP_TOT
 FROM T2
 GROUP BY 1,2          
""")

if metric_selection == "SAFE":
 print(agp_query)
 agp_sp = bc.read_gcp_table(agp_query)
 agp_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(AGP)
combined_txns_query = (
f"""
WITH filtered_tf AS (
 SELECT
   TXN_ID,
   TXN_DTE,
   CARD_NBR,
   SUM(GROSS_AMT) AS REVENUE,
   SUM(NET_AMT+MKDN_WOD_ALLOC_AMT+MKDN_POD_ALLOC_AMT) AS NET_SALES,
   SUM(ITEM_QTY) AS ITEMS
 FROM {TXN_FACTS}
 WHERE
   TXN_DTE >= {start_date} 
   AND TXN_DTE < {end_date}
   AND MISC_ITEM_QTY = 0
   AND DEPOSIT_ITEM_QTY = 0
   AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
 GROUP BY TXN_ID, TXN_DTE, CARD_NBR
)
SELECT
 smv.HOUSEHOLD_ID,
 tf.TXN_DTE,
 tf.TXN_ID,
 CASE
   WHEN f.REGISTER_NBR IN (99, 104, 173, 174, 999) THEN 'ECOMM'
   WHEN f.REGISTER_NBR IN (
     1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
     11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
     49, 50, 51, 52, 53, 54, 93, 94, 95, 96, 97, 98,
     116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
     151, 152, 153, 154, 175, 176, 177, 178, 179, 180,
     181, 182, 195, 196, 197, 198
   ) THEN 'STORE'
   ELSE NULL
 END AS TXN_LOCATION,
 tf.REVENUE,
 tf.NET_SALES,
 tf.ITEMS,
 f.TENDER_AMT_FOODSTAMPS + f.TENDER_AMT_EBT AS SNAP_TENDER
FROM filtered_tf AS tf
JOIN {TXN_HDR_COMBINED} AS f
 ON tf.TXN_ID = f.TXN_ID AND tf.TXN_DTE = f.TXN_DTE
JOIN {LU_DAY_MERGE} AS b
 ON CAST(f.TXN_DTE AS DATE) = b.D_DATE
JOIN (
 SELECT DISTINCT HOUSEHOLD_ID, LOYALTY_PROGRAM_CARD_NBR
 FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}
) AS smv
 ON SAFE_CAST(tf.CARD_NBR AS BIGNUMERIC) = SAFE_CAST(smv.LOYALTY_PROGRAM_CARD_NBR AS BIGNUMERIC)
WHERE
 f.TXN_HDR_SRC_CD = 0
 AND f.REGISTER_NBR IN (
   99, 104, 173, 174, 999,
   1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
   11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
   49, 50, 51, 52, 53, 54, 93, 94, 95, 96, 97, 98,
   116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
   151, 152, 153, 154, 175, 176, 177, 178, 179, 180,
   181, 182, 195, 196, 197, 198
 )
"""
  )

if metric_selection == "SAFE":
  print(combined_txns_query)
  combined_txns_sp = bc.read_gcp_table(combined_txns_query)
  combined_txns_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(COMBINED_TXNS)
clips_query = (
      f"""
            SELECT DISTINCT a.household_id, client_offer_id, clip_ts
            , CASE 
                  WHEN j.program_type = 'PERSONALIZATION EXPERIMENT' AND j.PROGRAM_CODE = 'SPD' THEN 'PZN'
                  WHEN j.program_type = 'CONTINUITY' and upper(j.PROGRAM_SUBTYPE) in ('MEAT SEAFOOD','PRODUCE','DAIRY','FROZEN') then 'PZN'
                  WHEN j.program_type <> 'PERSONALIZATION EXPERIMENT' AND j.program_type <> 'CONTINUITY' AND j.PROGRAM_CODE = 'SPD' THEN 'SPD'
                  ELSE OFFER_TYPE
                  END AS OFFER_TYPE_MOD
            FROM {LOY_CLIPS} as a
            LEFT JOIN (SELECT DISTINCT program_type, PROGRAM_SUBTYPE, PROGRAM_CODE, offer_id FROM {OFFER_BANK}) AS j 
                  ON a.client_offer_id = j.offer_id
            WHERE (a.clip_dt >= ({start_date}) AND a.clip_dt <= {end_date})
      """
)

if metric_selection == "SAFE":
      print(clips_query)
      clips_sp = bc.read_gcp_table(clips_query)
      clips_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(CLIPS)
redemptions_query = (
  f"""
    SELECT 
      r.TXN_ID
      , r.HOUSEHOLD_ID
      , r.TXN_DTE
      , r.CLIENT_OFFER_ID
      , CASE 
          WHEN j.program_type = 'PERSONALIZATION EXPERIMENT' AND j.PROGRAM_CODE = 'SPD' THEN 'PZN'
          WHEN j.program_type = 'CONTINUITY' and upper(j.PROGRAM_SUBTYPE) in ('MEAT','SEAFOOD','PRODUCE','DAIRY','FROZEN') then 'PZN'
          WHEN j.program_type <> 'PERSONALIZATION EXPERIMENT' AND j.program_type <> 'CONTINUITY' AND j.PROGRAM_CODE = 'SPD' THEN 'SPD' 
          ELSE r.offer_type
        END AS offer_type_mod
      , CASE 
          WHEN FUEL_REWARD_POINTS > 0 THEN -(FUEL_REWARD_POINTS) * 0.01 
          ELSE (MKDN_AMT + WOD_MKDN_AMT + POD_MKDN_AMT) 
        END AS MKDN
      FROM {ONLINE_REDEEMING_HHS} AS r
      LEFT JOIN (
                  SELECT DISTINCT 
                    PROGRAM_TYPE, 
                    PROGRAM_CODE, 
                    PROGRAM_SUBTYPE,
                    OFFER_ID 
                  FROM {OFFER_BANK}) AS j 
        ON (r.CLIENT_OFFER_ID = j.OFFER_ID)
      WHERE (r.TXN_DTE >= ({start_date}) AND r.TXN_DTE < ({end_date}))
  """
  )

if metric_selection == "SAFE":
  print(redemptions_query)
  redemptions_sp = bc.read_gcp_table(redemptions_query)
  redemptions_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(REDEMPTIONS)
gas_query = (
  f"""
WITH
GAS_TXNS AS(
  SELECT 
    SAFE_CAST(smv.HOUSEHOLD_ID as INT) as HOUSEHOLD_ID
    , f.TXN_DTE
    , SAFE_CAST(f.TXN_ID AS INT) AS TXN_ID
    , SUM(f.TOTAL_GROSS_AMT) AS REVENUE
  FROM {TXN_HDR_COMBINED} AS f
  JOIN (SELECT DISTINCT HOUSEHOLD_ID, LOYALTY_PROGRAM_CARD_NBR FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}) as smv
    ON SAFE_CAST(f.CARD_NBR AS NUMERIC) = SAFE_CAST(smv.LOYALTY_PROGRAM_CARD_NBR AS NUMERIC)
  WHERE 1=1 
    AND (f.TXN_DTE >= ({start_date}) AND f.TXN_DTE < ({end_date}))
    AND f.TXN_HDR_SRC_CD = 0
    AND f.REGISTER_NBR IN (44,45,46,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78) 
  GROUP BY 1,2,3
),

OWN_MKDN AS(
select rt.HOUSEHOLD_ID
      , SUM(rt.reward_token_offered_qty) as Rewards_redeemed
      , sum(rt.total_savings_value_amt) as Mkdn
FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.BUSINESS_PARTNER_REWARD_TRANSACTION as rt   --EDM native table
JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.BUSINESS_PARTNER as bp  
  ON rt.business_partner_integration_id = bp.business_partner_integration_id
LEFT JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_Store as str 
  on cast(bp.partner_site_id as int64) = str.store_id
  and str.division_id in (5,17,19,20,23,24,25,27,29,30,32,33,34,35)
WHERE rt.transaction_type_cd = 'REDREQ'
  and (cast(rt.transaction_ts AS date) >= ({start_date}) and cast(rt.transaction_ts AS date) < ({end_date}))
  and rt.dw_last_effective_dt = '9999-12-31'
  and rt.reward_token_offered_qty < 0
  and safe_Cast(rt.partner_division_id as int64) <> 15
  and bp.dw_last_effective_dt = '9999-12-31'
  and safe_cast(bp.partner_id as int64) = 9999  --Own fuel stations
  group by all
),

PARTNER_BASE AS(
select  rt.HOUSEHOLD_ID
        , partner_id
        , case when safe_cast(partner_id as int64) = 3001  then 0.0395 else 0.035 end as multiplier
        , SUM(rt.reward_token_offered_qty) as Rewards_redeemed
        , sum(rt.total_purchase_qty) as gallons
        , sum(rt.total_savings_value_amt) as MKDN
FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.BUSINESS_PARTNER_REWARD_TRANSACTION as rt   --EDM native table
JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.BUSINESS_PARTNER as bp  
  ON rt.business_partner_integration_id = bp.business_partner_integration_id
LEFT JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_Store as str 
  on cast(bp.partner_site_id as int64) = str.store_id
  and str.division_id in (5,17,19,20,23,24,25,27,29,30,32,33,34,35)
WHERE rt.transaction_type_cd = 'REDREQ'
      and (cast(rt.transaction_ts AS date) >= ({start_date}) and cast(rt.transaction_ts AS date) < ({end_date}))
      and rt.dw_last_effective_dt = '9999-12-31'
      and rt.reward_token_offered_qty < 0
      and safe_Cast(rt.partner_division_id as int64) <> 15
      and bp.dw_last_effective_dt = '9999-12-31'
      and safe_cast(bp.partner_id as int64) <> 9999  --Partner fuel stations
      group by all
),

PARTNER_MKDN as(
SELECT HOUSEHOLD_ID
      , SUM(Rewards_redeemed) as Rewards_redeemed
      , SUM(Gallons) as gallons
      , SUM(MKDN) as mkdn
      ,sum(Mkdn) - sum(gallons * multiplier) as net_mkdn
FROM PARTNER_BASE
group by all
)

SELECT g.*
      , COALESCE(SUM(o.MKDN),0) as own_MKDN
      , COALESCE(SUM(p.MKDN),0) as partner_MKDN
      , COALESCE(SUM(o.MKDN),0)+COALESCE(SUM(p.net_MKDN),0) as GAS_MKDN
      , -1*(COALESCE(SUM(o.Rewards_redeemed),0)+COALESCE(SUM(p.Rewards_redeemed),0)) as GAS_REWARD_REDEMPTIONS
FROM GAS_TXNS as g
LEFT JOIN OWN_MKDN as o
  on g.HOUSEHOLD_ID = o.HOUSEHOLD_ID
LEFT JOIN PARTNER_MKDN as p
  on g.HOUSEHOLD_ID = p.HOUSEHOLD_ID
  WHERE o.MKDN > 0 or p.MKDN > 0
group by all
  """
)

if metric_selection == "SAFE":
  print(gas_query)
  gas_txns_sp = bc.read_gcp_table(gas_query)
  gas_txns_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(GAS_TXNS)
basket_time_query = (
  f"""
    SELECT *, TIMESTAMP_DIFF(DATETIME(VISIT_END),DATETIME(VISIT_ST),MINUTE) AS BASKET_BUILD_TIME
      FROM(
        SELECT 
            CONCAT(POST_VISID_HIGH, POST_VISID_LOW, VISIT_NUM, VISIT_START_TIME_GMT) AS VISIT_ID,
            post_evar49 AS ADOBE_VISITOR_ID,
            post_evar47 AS HOUSEHOLD_ID,
            MIN(date_time) AS VISIT_ST,
            MAX(date_time) AS VISIT_END
        FROM {CLICK_HIT_DATA}
        WHERE 1=1
            AND exclude_hit = '0' -- Hit excluded by client rule
            AND post_evar90 <> 'web'
            AND hit_source NOT IN ('5','7','8','9')
            AND LOWER(TRIM(post_evar47)) not like '%id not found%'
            AND LOWER(TRIM(post_evar47)) not like '%-%'
            AND SUBSTR(TRIM(post_evar47),1,1) in ('0','1','2','3','4','5','6','7','8','9')
            AND (DATETIME(DATE_TIME) >= ({start_date}) AND DATETIME(DATE_TIME) < ({end_date}))
            AND CONCAT(',', POST_EVENT_LIST, ',') LIKE '%,12,%'
        GROUP BY 1,2,3
        HAVING COUNT(CONCAT(',', POST_EVENT_LIST, ','))>=2
      )
  """
)

if metric_selection == "SAFE":
  print(basket_time_query)
  basket_time_sp = bc.read_gcp_table(basket_time_query)
  basket_time_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(BASKET_TIME_TXNS)
bnc_query = (
  f"""
with Table_C360 as(
select RETAIL_CUSTOMER_UUID , safe_cast(CLUB_CARD_NBR as int) as CLUB_CARD_NBR , safe_cast(HOUSEHOLD_ID as int) as HOUSEHOLD_ID
from {C360_CUSTOMER_PROFILE}
),

Table_First_Events as(
SELECT a.retail_customer_uuid, a.FIRST_DUG_DT,a.FIRST_DELIVERY_DT,
case 
  when FIRST_DELIVERY_DT is not NULL AND FIRST_DUG_DT is not NULL then LEAST(FIRST_DELIVERY_DT, FIRST_DUG_DT)
  else COALESCE(FIRST_DELIVERY_DT, FIRST_DUG_DT)
  end as FIRST_ECOMM_DT
from gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_CUSTOMER_FIRST_EVENT A
where A.DW_LOGICAL_DELETE_IND = FALSE
and A.DW_CURRENT_VERSION_IND = TRUE
and (FIRST_DELIVERY_DT is not NULL or FIRST_DUG_DT is not NULL)
)

select B.HOUSEHOLD_ID, 'BNC TO ECOMM' as BNC_SEGMENT ,MIN(A.FIRST_ECOMM_DT) AS TXN_DTE
from Table_First_Events as A
join Table_C360 as B
  on A.RETAIL_CUSTOMER_UUID = B.RETAIL_CUSTOMER_UUID
WHERE (FIRST_ECOMM_DT >= ({start_date}) AND FIRST_ECOMM_DT < ({end_date}))
group by all
  """ )

if metric_selection == "SAFE":
  print(bnc_query)
  bnc_sp = bc.read_gcp_table(bnc_query)
  bnc_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(BNC)
basket_health_query = (
    f"""
        SELECT SAFE_CAST(HHS_ID as INT) as HOUSEHOLD_ID
            , cs.TXN_DTE
            , bh.OVERALL_CATEGORY
            , cs.TXN_ID
        FROM {ESA_HOLISTIC_SCORECARD_BASKET_HEALTH} as bh
        JOIN {ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT} as cs
            ON cs.TXN_ID = bh.TXN_ID 
        WHERE 1=1
            AND cs.SEGMENT_1 IS NOT NULL
            AND (cs.TXN_DTE >= ({start_date}) AND cs.TXN_DTE < ({end_date}) )
        GROUP BY ALL
    """
    )

if metric_selection == "SAFE":
    print(basket_health_query)
    basket_health = bc.read_gcp_table(basket_health_query)
    basket_health.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(BASKET_HEALTH)
txn_cat_query = f"""
WITH filtered_txn AS (
 SELECT
   TXN_ID,
   TXN_DTE,
   CARD_NBR,
   STORE_ID,
   UPC_ID,
   GROSS_AMT,
   ITEM_QTY,
   MEAS_QTY
 FROM {TXN_FACTS}
 WHERE
   TXN_DTE >= {start_date}
   AND TXN_DTE < {end_date}
   AND TXN_HDR_SRC_CD = 0
   AND MISC_ITEM_QTY = 0
   AND DEPOSIT_ITEM_QTY = 0
   AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
),
filtered_upc AS (
 SELECT *
 FROM {D1_UPC}
 WHERE
   SAFEWAY_UPC_IND = TRUE
   AND CORPORATION_ID = '001'
   AND RETAIL_DEPARTMENT_ID NOT IN ('339', '347')
   AND SAFE_CAST(SMIC_GROUP_ID AS STRING) NOT IN ('0', '2', '98', '99')
   AND DW_LOGICAL_DELETE_IND = FALSE
   AND RETAIL_DEPARTMENT_ID <> 'N/A'
),
filtered_store AS (
 SELECT *
 FROM {D1_RETAIL_STORE}
 WHERE
   DW_LOGICAL_DELETE_IND = FALSE
   AND PARENT_OPERATING_AREA_CD NOT IN ('N/A')
),
household_map AS (
 SELECT DISTINCT
   HOUSEHOLD_ID,
   LOYALTY_PROGRAM_CARD_NBR
 FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}
 WHERE SAFE_CAST(HOUSEHOLD_ID AS BIGNUMERIC) > 0
)
SELECT
 t.TXN_ID,
 smv.HOUSEHOLD_ID,
 t.TXN_DTE,
 h.SMIC_CATEGORY_ID,
 h.SMIC_CATEGORY_DSC,
 h.DEPARTMENT_NM,
 COALESCE(SUM(t.GROSS_AMT), 0) AS REVENUE,
 COALESCE(SUM(t.ITEM_QTY), 0) AS ITEM_QTY
FROM filtered_txn AS t
JOIN filtered_upc AS h
 ON SAFE_CAST(t.UPC_ID AS BIGNUMERIC) = SAFE_CAST(h.UPC_NBR AS BIGNUMERIC)
JOIN filtered_store AS str
 ON SAFE_CAST(t.STORE_ID AS BIGNUMERIC) = SAFE_CAST(str.RETAIL_STORE_FACILITY_NBR AS BIGNUMERIC)
JOIN household_map AS smv
 ON SAFE_CAST(t.CARD_NBR AS BIGNUMERIC) = SAFE_CAST(smv.LOYALTY_PROGRAM_CARD_NBR AS BIGNUMERIC)
GROUP BY ALL
"""
if metric_selection == "SAFE":
   print(txn_cat_query)
   txn_cat_sp = bc.read_gcp_table(txn_cat_query)
   txn_cat_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(CATEGORY_TXNS)
acc_health_query = (f"""
WITH email_contacts AS (
   SELECT DISTINCT b.household_id
   FROM {CUSTOMER_DIGITAL_CONTACT} a
   JOIN {C360_CUSTOMER_PROFILE} b
   ON a.retail_customer_uuid = b.retail_customer_uuid
   WHERE a.dw_current_version_ind = true
   AND email_id IS NOT NULL
   AND email_id NOT LIKE 'L/km'
),
phone_contacts AS (
   SELECT DISTINCT b.household_id
   FROM {CUSTOMER_PHONE_FAX_CONTACT} a
   JOIN {C360_CUSTOMER_PROFILE} b
   ON a.retail_customer_uuid = b.retail_customer_uuid
   WHERE b.dw_current_version_ind = true
   AND b.phone_nbr IS NOT NULL
   AND b.phone_nbr NOT LIKE 'O/zG'
),

fn_ln_contacts AS (
   SELECT DISTINCT household_id
   FROM {C360_CUSTOMER_PROFILE}
   WHERE first_nm IS NOT NULL
   AND first_nm NOT IN ('i/ss','5mPP5n BVAcZ63Un','o1iORzK','o8B Gyfvt0uf','4k5j','IWlXYgMw','kAeT','5mPP5n BVAcZ63Un','Ht5k','4xVEbcd','KyF')
   AND last_nm IS NOT NULL
   AND last_nm NOT IN ('i/ss','Z5kY4REK','8m2ZAuy','IWlXYgMw','kAeT','5mPP5n BVAcZ63Un','o9O')
   AND dw_current_version_ind = true
),
bday_contacts AS (
   SELECT household_id
   FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}
   WHERE birth_dt IS NOT NULL
   AND birth_dt NOT LIKE 'i/ss'
),
address_contacts AS (
   SELECT DISTINCT household_id
   FROM {C360_CUSTOMER_PROFILE}
   WHERE zipcode IS NOT NULL
   AND zipcode <> 'N/A'
   AND dw_current_version_ind = true
),
smv_employee AS (
   SELECT DISTINCT household_id
   FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}
   where employee_ind = true
),
c360_employee AS (
   SELECT DISTINCT household_id
   FROM {C360_CUSTOMER_PROFILE}
   where employee_ind = true
),
division_info as(
SELECT DISTINCT C.household_id, C.prim_division_id, D.division_nm as prim_division_nm
from (select household_id, prim_division_id from {C360_CUSTOMER_PROFILE} where PRIM_DIVISION_ID is not NULL) C
left JOIN (select division_id, division_nm from {C360_STORE}) D
    on SAFE_CAST(C.PRIM_DIVISION_ID AS NUMERIC) = SAFE_CAST(D.DIVISION_ID AS NUMERIC)
)
select X.*, Y.prim_division_id, Y.prim_division_nm
from
(SELECT 
   cp.household_id as household_id,
   COUNT(DISTINCT ec.household_id) AS email_ind,
   COUNT(DISTINCT pc.household_id) AS phone_ind,
   CAST(COUNT(DISTINCT fnln.household_id) / 2 AS INT) AS fn_ln_ind,
   COUNT(DISTINCT bd.household_id) AS bday_ind,
   COUNT(DISTINCT ad.household_id) AS address_ind,
   COUNT(DISTINCT smve.household_id) AS smv_emp_ind,
   COUNT(DISTINCT c360e.household_id) AS c360_emp_ind,
FROM {C360_CUSTOMER_PROFILE} cp
LEFT JOIN email_contacts ec ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(ec.household_id AS NUMERIC)
LEFT JOIN phone_contacts pc ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(pc.household_id AS NUMERIC)
LEFT JOIN fn_ln_contacts fnln ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(fnln.household_id AS NUMERIC)
LEFT JOIN bday_contacts bd ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(bd.household_id AS NUMERIC)
LEFT JOIN address_contacts ad ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(ad.household_id AS NUMERIC)
LEFT JOIN smv_employee smve ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(smve.household_id AS NUMERIC)
LEFT JOIN c360_employee c360e ON SAFE_CAST(cp.household_id AS NUMERIC) = SAFE_CAST(c360e.household_id AS NUMERIC)
GROUP BY cp.household_id) as X
left join
division_info as Y
on SAFE_CAST(X.household_id AS NUMERIC) = SAFE_CAST(Y.household_id AS NUMERIC)
""")

if metric_selection == "SAFE":
   print(acc_health_query)
   acc_health_sp = bc.read_gcp_table(acc_health_query)
   acc_health_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(EXP_COE_ACC_HEALTH)
# exp_coe_hh_query = f'''
#   select HOUSEHOLD_ID
#   from {EXP_COE_3P_HHS_LIST}
#   '''

# if metric_selection == "SAFE":
#    print(exp_coe_hh_query)
#    exp_coe_3p_hh = bc.read_gcp_table(exp_coe_hh_query)
#    exp_coe_3p_hh.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(EXP_COE_3P_HHS_LIST_DB_WORK)
%md
--------------------------------------------
# POWER METRICS
1. If Start and End dates are not provided, then defaults to the past 365 days. 
2. Cannot provide a custom experiment ID for custom runs.
if metric_selection == "Power":
  exposure_visit_for_power_query = (
    f"""
      SELECT
        TIMESTAMP(VISIT_START_TS) AS VISIT_START_TS,
        VISITOR_ID,
        SAFE_CAST(RETAIL_CUSTOMER_HHID_TXT AS INT) AS RETAIL_CUSTOMER_HHID_TXT,
        ADOBE_VISITOR_ID_TXT,
        SIGN_IN_IND,
        BANNER_NM_TXT AS BANNER_NM,
        LOWER(USER_OS_TYPE_CD) AS USER_OS_TYPE_CD,
        PAGES_VIEWED_TXT
        FROM 
          {CLICK_STREAM_VISIT_VIEW}
        WHERE 1=1 
          AND ( DATE(VISIT_START_TS) > {start_date_tz} AND DATE(VISIT_START_TS) < {end_date_tz} )
    """)

  print(exposure_visit_for_power_query)
  exposure_visit_for_power_sp = bc.read_gcp_table(exposure_visit_for_power_query)
  exposure_visit_for_power_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(EXPOSURE_VISIT_FOR_POWER)
if metric_selection == "Power":  
  txn_for_power_query = (
    f"""
WITH filtered_txns AS (
SELECT
   TXN_ID,
   TXN_DTE,
   CARD_NBR,
   SUM(GROSS_AMT) AS REVENUE,
   SUM(NET_AMT + MKDN_WOD_ALLOC_AMT + MKDN_POD_ALLOC_AMT) AS NET_SALES,
   SUM(ITEM_QTY) AS ITEMS
FROM {TXN_FACTS}
WHERE
   TXN_DTE >= DATE({start_date})
   AND TXN_DTE < DATE({end_date})
   AND REV_DTL_SUBTYPE_ID IN (0, 6, 7)
   AND MISC_ITEM_QTY = 0
   AND DEPOSIT_ITEM_QTY = 0
GROUP BY TXN_ID, TXN_DTE, CARD_NBR
)
SELECT
SAFE_CAST(smv.HOUSEHOLD_ID AS INT64) AS HOUSEHOLD_ID,
tf.TXN_DTE,
tf.TXN_ID,
tf.REVENUE,
tf.NET_SALES,
tf.ITEMS,
f.TENDER_AMT_FOODSTAMPS + f.TENDER_AMT_EBT AS SNAP_TENDER
FROM filtered_txns AS tf
JOIN {TXN_HDR_COMBINED} AS f
ON tf.TXN_ID = f.TXN_ID AND tf.TXN_DTE = f.TXN_DTE
JOIN {LU_DAY_MERGE} AS b
ON CAST(tf.TXN_DTE AS DATE) = b.D_DATE
JOIN (
SELECT DISTINCT HOUSEHOLD_ID, LOYALTY_PROGRAM_CARD_NBR
FROM {SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD}
) AS smv
ON SAFE_CAST(tf.CARD_NBR AS BIGNUMERIC) = SAFE_CAST(smv.LOYALTY_PROGRAM_CARD_NBR AS BIGNUMERIC)
WHERE
f.TXN_HDR_SRC_CD = 0
    """)
 
  #print(txn_for_power_query)
  txn_for_power_sp = bc.read_gcp_table(txn_for_power_query)
  txn_for_power_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TXN_FOR_POWER)
%md
--------------------------------------------
# ATTRIBUTES
1. Dates are irrelevant for these queries.  If dates are provided as inputs, it will not change the results.
2. Cannot provide a custom experiment ID.
if metric_selection == "Attributes":
  push_opt_in_query = (
    f"""
      SELECT 
        DISTINCT 
          SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID, 
          'push' AS DIGITAL_REACH 
      FROM {SFMC_PUSH_POPULATION}
    """)

  print(push_opt_in_query)
  push_opt_in_sp = bc.read_gcp_table(push_opt_in_query)
  push_opt_in_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(PUSH_OPT_IN)
if metric_selection == "Attributes":
  email_opt_in_query = (
    f"""
      SELECT 
        DISTINCT 
          SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID, 
          'email' AS DIGITAL_REACH
      FROM {LEAP}
    """)

  print(email_opt_in_query)
  email_opt_in_sp = bc.read_gcp_table(email_opt_in_query)
  email_opt_in_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(EMAIL_OPT_IN)
if metric_selection == "Attributes":
  sms_query = (
    f"""
      SELECT DISTINCT SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID
            , 'sms' AS DIGITAL_REACH
      FROM {SMS_POPULATION}
    """ )

  print(sms_query)
  sms_sp = bc.read_gcp_table(sms_query)
  sms_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SMS_OPT_IN)
if metric_selection == "Attributes":
  re_query = (
    f"""
    SELECT SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID, 1 as rewards_engaged
    from gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.C360_HH_ENG as c
    join gcp-abs-udco-bqvw-prod-prj-01.udco_ds_acct.D0_FISCAL_WEEK as d
      on c.fiscal_week_id = d.fiscal_week_id
    where (gr_redemptions > 0 or gas_redemptions > 0)
    and fiscal_week_start_dt between current_date() - 84 and current_date() 
    group by all
    """ )

  print(re_query)
  re_sp = bc.read_gcp_table(re_query)
  re_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(REWARDS_ENGAGED)
if metric_selection == "Attributes":
  facts_segment_query = (
    f"""
    WITH LatestWeek AS (
   -- Fetch the latest week's FACTS category for each household
   SELECT  
       SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID,
       CASE
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (1,2) THEN 'Elite and Best'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (3,4) THEN 'Good'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (5,6) THEN 'Occasional'
           ELSE 'unknown'
       END AS FACTS
   FROM {FACTS_SEGMENT_WEEK}
   QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID ORDER BY WEEK_ID DESC) = 1
),
Last12Weeks AS (
   -- Step 1: Get data from the last 12 weeks
   SELECT
       SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID,
       FACTS_LEVEL2_SEGMENT_ID
   FROM {FACTS_SEGMENT_WEEK}
   WHERE WEEK_ID >= (SELECT MAX(WEEK_ID) FROM {FACTS_SEGMENT_WEEK}) - 11
),
ModeCalculation AS (
   -- Step 2: Compute mode (most frequent FACTS_LEVEL2_SEGMENT_ID per household)
   SELECT
       HOUSEHOLD_ID,
       FACTS_LEVEL2_SEGMENT_ID,
       ROW_NUMBER() OVER (PARTITION BY HOUSEHOLD_ID ORDER BY COUNT(*) DESC, FACTS_LEVEL2_SEGMENT_ID ASC) AS rn
   FROM Last12Weeks
   WHERE FACTS_LEVEL2_SEGMENT_ID IS NOT NULL
   GROUP BY HOUSEHOLD_ID, FACTS_LEVEL2_SEGMENT_ID
),
Last12WeeksFacts AS (
   -- Step 3: Select the most frequent segment per household and categorize it
   SELECT
       HOUSEHOLD_ID,
       CASE
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (1,2) THEN '1.Elite and Best'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (3,4) THEN '2.Good'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (5,6) THEN '3.Occasional'
           ELSE 'unknown'
       END AS TWELVE_WEEKS_FACTS
       ,
      CASE
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (1) THEN '1.Elite'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (2) THEN '2.Best'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (3,4) THEN '3.Good'
           WHEN FACTS_LEVEL2_SEGMENT_ID IN (5,6) THEN '4.Occasional'
           ELSE 'unknown'
       END AS TWELVE_WEEKS_FACTS_DETAIL
   FROM ModeCalculation
   WHERE rn = 1
)   
-- Final SELECT: Use FULL OUTER JOIN to prevent NULLs in mode facts column
SELECT
   COALESCE(L.HOUSEHOLD_ID, W.HOUSEHOLD_ID) AS HOUSEHOLD_ID,            -- Ensure all unique HHids
   COALESCE(L.FACTS, 'unknown') AS FACTS,                               -- If latest week data is missing, set to 'unknown'
   COALESCE(W.TWELVE_WEEKS_FACTS, 'unknown') AS TWELVE_WEEKS_FACTS,     -- If no mode found, set to 'unknown'
   COALESCE(W.TWELVE_WEEKS_FACTS_DETAIL, 'unknown') AS TWELVE_WEEKS_FACTS_DETAIL
FROM LatestWeek L
FULL OUTER JOIN Last12WeeksFacts W
ON L.HOUSEHOLD_ID = W.HOUSEHOLD_ID
""")

  #print(facts_segment_query)
  fact_segment_sp = bc.read_gcp_table(facts_segment_query)
  fact_segment_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(FACT_SEGMENTS)
if metric_selection == "Attributes":
  myneeds_segment_query = (
    f"""
      SELECT  
      SAFE_CAST(HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID,
      case when segment = 1 then '1.Easy Shoppers'
           when segment = 2 then '2.Easy Eaters'
           when segment = 3 then '3.Healthy Foodies'
           when segment = 4 then '4.Scratch Foodies'
           when segment = 5 then '5.Chasing Price'
           when segment = 6 then '6.One-Stop Low Price'     
      END AS MYNEEDS
      FROM {CA_MA_ATTITUDINAL_SEGMENTATION_MASTER}
      QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID ORDER BY START_WEEK_ID DESC) = 1
      """)

  print(myneeds_segment_query)
  myneeds_segment_sp = bc.read_gcp_table(myneeds_segment_query)
  myneeds_segment_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(MYNEEDS_SEGMENTS)
if metric_selection == "Attributes":
  primary_platform_query = (
  f"""
    WITH
      BASE_DIGITAL AS(
        SELECT 
          RETAIL_CUSTOMER_HHID_TXT AS HOUSEHOLD_ID,
          CASE 
              WHEN LOWER(USER_OS_TYPE_CD) LIKE '%app%' THEN 'App' 
              ELSE 'Web' 
          END AS DIGITAL_INTERACTION,  
          COUNT(*) AS DIGITAL_TRAFFIC
      FROM {EXPOSURE_VISIT_FOR_POWER} AS a
      WHERE 1=1
          AND RETAIL_CUSTOMER_HHID_TXT IS NOT NULL
      GROUP BY 1,2
      ORDER BY 1,2
      ),

      INTERIM_BASE_DIGITAL AS(
        SELECT
          COALESCE(a.HOUSEHOLD_ID,b.HOUSEHOLD_ID) AS HOUSEHOLD_ID,
          COALESCE(a.DIGITAL_TRAFFIC,0) AS WEB_TRAFFIC,
          COALESCE(b.DIGITAL_TRAFFIC,0) AS UMA_TRAFFIC
        FROM
          (SELECT * FROM BASE_DIGITAL WHERE DIGITAL_INTERACTION = 'Web') AS a
        FULL OUTER JOIN 
          (SELECT * FROM BASE_DIGITAL WHERE DIGITAL_INTERACTION = 'App') AS b
        ON a.HOUSEHOLD_ID = b.HOUSEHOLD_ID 
      )

    SELECT
      HOUSEHOLD_ID,
      WEB_TRAFFIC,
      UMA_TRAFFIC,
      WEB_TRAFFIC + UMA_TRAFFIC AS TOTAL_DIGITAL_TRAFFIC,
      WEB_TRAFFIC/(WEB_TRAFFIC + UMA_TRAFFIC) AS PCT_WEB,
      UMA_TRAFFIC/(WEB_TRAFFIC + UMA_TRAFFIC) AS PCT_UMA,
      CASE 
        WHEN WEB_TRAFFIC/(WEB_TRAFFIC + UMA_TRAFFIC) < 0.3 THEN 'UMA'
        WHEN WEB_TRAFFIC/(WEB_TRAFFIC + UMA_TRAFFIC) > 0.7 THEN 'WEB'
        ELSE 'BOTH'
      END AS PRIMARY_PLATFORM
    FROM INTERIM_BASE_DIGITAL
  """)

  print(primary_platform_query)
  primary_platform_sp = spark.sql(primary_platform_query)
  primary_platform_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(PRIMARY_PLATFORM)
if metric_selection == "Attributes":
  division_mapping_query = (
    f"""
      WITH   
        DIVISION_MAPPING AS(
          SELECT 
            DISTINCT SAFE_CAST(DIVISION_ID AS INT) AS DIVISION_ID, DIVISION_NM, BANNER_NM,
            safe_cast(RETAIL_STORE_FACILITY_NBR as int) as STORE_ID
          FROM {D1_RETAIL_STORE}
          WHERE DIVISION_NM NOT IN ('N/A','TEST') 
          AND DW_LOGICAL_DELETE_IND = FALSE
          ORDER BY 1 
      )

      SELECT
            SAFE_CAST(a.HOUSEHOLD_ID AS INT) AS HOUSEHOLD_ID,
            SAFE_CAST(a.PGM_PERIOD_ID AS INT) AS PGM_PERIOD_ID,
            SAFE_CAST(a.PRIM_STORE_ID AS INT) AS PRIM_STORE_ID,
            SAFE_CAST(b.DIVISION_ID AS INT) AS DIVISION_ID,
            b.DIVISION_NM, b.BANNER_NM
      FROM
            {JAA_HH_DIV_PRIMARY_STORE} AS a
      LEFT JOIN DIVISION_MAPPING AS b
        ON safe_cast(a.PRIM_STORE_ID as int) = safe_Cast(b.STORE_ID as int)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID ORDER BY PGM_PERIOD_ID DESC) = 1
      """)

  print(division_mapping_query)
  division_mapping_sp = bc.read_gcp_table(division_mapping_query)
  division_mapping_sp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(DIVISION_MAPPING)