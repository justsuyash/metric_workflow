# Databricks notebook source
# DBTITLE 1,Imports
# MAGIC %run /Workspace/Projects/Experimentation/aaml-experimentation-coe/exp_coe_utils

# COMMAND ----------

# DBTITLE 1,Notebook set up
##---------------------------------------------------------------------------------------------------------------------------##
## System & Notebook Setup                                                                                                   

## System Settings
import pandas as pd
pd.set_option('display.max_colwidth', 0)

## Notebook Input Widgets

# dbutils.widgets.text("01. Experiment Identifier", "ACIP-20404") 
# dbutils.widgets.text("02. Start Date", "2024-01-10") 
# dbutils.widgets.text("03. End Date", "2024-01-22") 
# dbutils.widgets.text("04. Significance Level", defaultValue="0.1")
# dbutils.widgets.dropdown("05. Experiment Platform", "Adobe", ["Adobe", "Decision Engine", "Push", "Email", "Household IDs"])
# dbutils.widgets.text("06. Page Filter", "")
# dbutils.widgets.dropdown("07. Run Exposure Query", "False", ["True", "False"])
# dbutils.widgets.dropdown("08. Metric Queries", "Standard", ["Standard", "DBFS Link", "Custom"])
# dbutils.widgets.text("09. DBFS Link", "")
# dbutils.widgets.combobox("10. Segmentation", "", ["FACTS", "MYNEEDS", "BNC", "FACTS-ExposureTable", "MyNeeds-ExposureTable"])
# dbutils.widgets.dropdown("11. Exposure Filter", "None", ["BNC", "SNAP", "Page", "Custom", "None"])
# dbutils.widgets.multiselect("12. OS Platform", "None", ["iOS", "Android","Web","None"])
# dbutils.widgets.multiselect("13. Exclude Banner Filter", "", ["", "pavilions", "safeway","andronico","albertsons","jewel-osco","vons","carrsqc","acme","tomthumb","randalls","shaws","balduccis", "haggen" ,"kingsfoodmarkets","acmemarkets" ])
# dbutils.widgets.dropdown("14. Winsorization", defaultValue="99th", choices=["99th", "OFF"])

#dbutils.widgets.removeAll()

##---------------------------------------------------------------------------------------------------------------------------##   

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# DBTITLE 1,Optional Get Adobe Experiment ID from ACIP#
### Obtain the Adobe Experiment ID (Campaign ID)
### If an ACIP# is input into the '01. Experiment Identifier' field, then this will look up the Campaign ID by using the CAMPAIGN_DSC field in the ADOBE_TNT table.
### If an Adobe Experiment (Campaign ID) is provided, then this will check the ADOBE_TNT table to ensure that it is a valid ID

if getArgument("05. Experiment Platform") == 'Adobe':
  if getArgument("01. Experiment Identifier").split('-')[0] == 'ACIP':
    experiment_id_query = (
        f"""
        SELECT DISTINCT
          CAST(CAMPAIGN_ID AS INT) AS CAMPAIGN_ID, RECIPE_NM
        FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.ADOBE_TNT
        WHERE CAMPAIGN_DSC LIKE '%{getArgument("01. Experiment Identifier")}%'
        """
      )
  else:
    experiment_id_query = (
        f"""
        SELECT DISTINCT
          CAST(CAMPAIGN_ID AS INT) AS CAMPAIGN_ID, RECIPE_NM
        FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.ADOBE_TNT
        WHERE CAST(CAMPAIGN_ID AS STRING) LIKE '%{getArgument("01. Experiment Identifier")}%'
        """
    )

  experiment_id_sp = bc.read_gcp_table(experiment_id_query)
  experiment_id_df = experiment_id_sp.select("*").toPandas()
  EXPERIMENT_ID = str(experiment_id_sp.collect()[0]['CAMPAIGN_ID'])
  
else:
  EXPERIMENT_ID = str(getArgument("01. Experiment Identifier").replace('-', '_'))

print('Experiment ID = {}'.format(EXPERIMENT_ID))

# COMMAND ----------

# DBTITLE 1,Get Inputs from Notebook
EXP_START_DATE = getArgument("02. Start Date")

### If a date in the future is provided for end date, then this will correct and run the notebook up until yesterday
if datetime.datetime.strptime(getArgument("03. End Date"), '%Y-%m-%d').date() >= datetime.date.today():
  EXP_END_DATE = str(datetime.date.today() - datetime.timedelta(days=1))
else:
  EXP_END_DATE = getArgument("03. End Date")

SIGNIFICANCE = float(getArgument("04. Significance Level"))
EXP_PLATFORM = getArgument("05. Experiment Platform")
PAGE_FILTER_INPUT = getArgument("06. Page Filter")
RUN_EXPOSURE = getArgument("07. Run Exposure Query")
METRIC_QUERIES = getArgument("08. Metric Queries")
DBFS_LINK = getArgument("09. DBFS Link")
SEGMENTATION = getArgument("10. Segmentation")
if SEGMENTATION:
  SEGMENTATION = SEGMENTATION + ',' 
EXPOSURE_FILTER = getArgument("11. Exposure Filter")
OS_PLATFORM = getArgument("12. OS Platform")
banner_selected = getArgument("13. Exclude Banner Filter")
WINSORIZE = getArgument("14. Winsorization")
control_variant_nm = 'VARIANT A'

# COMMAND ----------

# DBTITLE 1,Experiment Implementation
one_control = f"""CASE
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:0:0%' THEN 'VARIANT A'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:1:0%' THEN 'VARIANT B'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:2:0%' THEN 'VARIANT C'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:3:0%' THEN 'VARIANT D'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:4:0%' THEN 'VARIANT E'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:5:0%' THEN 'VARIANT F'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:6:0%' THEN 'VARIANT G'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:7:0%' THEN 'VARIANT H'
      ELSE NULL
    END AS VARIANT_ID"""

two_controls = f"""CASE
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:0:0%' THEN 'VARIANT A'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:1:0%' THEN 'VARIANT A'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:2:0%' THEN 'VARIANT B'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:3:0%' THEN 'VARIANT C'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:4:0%' THEN 'VARIANT D'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:5:0%' THEN 'VARIANT E'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:6:0%' THEN 'VARIANT F'
      WHEN post_tnt LIKE '%{EXPERIMENT_ID}:7:0%' THEN 'VARIANT G'
      ELSE NULL
    END AS VARIANT_ID"""

if EXP_PLATFORM == 'Adobe': 
  visitor_unit = 'ADOBE_VISITOR_ID'
  if any(experiment_id_df['RECIPE_NM'].str.contains('Control 2', case=True, regex=False)):
    control_strategy = two_controls
    print('2 controls')
  else:
    control_strategy = one_control
    print('1 control')
else:
  visitor_unit = 'HOUSEHOLD_ID'

if EXP_PLATFORM == 'Push':
  push_table = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.SQ_PUSH_DATA' 
elif EXP_PLATFORM == 'Email':
  email_table = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.SQ_EMAIL_DATA'

print('Experiment ID = {}'.format(EXPERIMENT_ID))

# COMMAND ----------

# DBTITLE 1,DBFS Link
### Use to direct the metric queries to a pre-run set of tables, identified by the DBFS_LINK

if METRIC_QUERIES == 'DBFS Link' and not DBFS_LINK:
   displayHTML("""<h3><font color="red"> !WARNING! The Metric Query input is set to 'DBFS Link', but did not provide a link. </font></h3>""")
     
print('Metric Query Approach: ', METRIC_QUERIES)

# COMMAND ----------

# DBTITLE 1,Page Filter
### Page filter can accept list of strings separated by "," or ", "
### Checkout consists of multiple steps and each can contain 'order confirmation'

if PAGE_FILTER_INPUT:
  page_split = PAGE_FILTER_INPUT.replace(", ",",").split(",")
  page_format = [f"'%{s}%'".replace(" ", "_") for s in page_split]
  page_string = ', '.join(page_format)
  
  if EXP_PLATFORM == 'Adobe': 
    page_filter = f"""AND (lower(post_pagename) LIKE ANY ({page_string})
    or lower(POST_EVAR151) LIKE ANY ({page_string})
    or lower(POST_EVAR152) LIKE ANY ({page_string})
    or lower(POST_EVAR153) LIKE ANY ({page_string}))"""
    print(page_filter)
  elif EXP_PLATFORM == 'Decision Engine':
    page_filter = f"""
      AND (lower(EVENT_TYPE_CD) LIKE '%pageloaded%' AND lower(EVENT_NM) LIKE {page_string})
    """
    print(page_filter)
  elif EXP_PLATFORM == 'Household IDs':
    page_filter = f"""AND PAGES_VIEWED_TXT LIKE ANY ({page_string})"""
    print(page_filter)
else:
  page_filter = ""
  print("No page filter")

# COMMAND ----------

# DBTITLE 1,Banner Filter
if banner_selected != 'None' and banner_selected.strip() != '':
   # Splitting the input string by commas and converting to lowercase
   selected_banners = banner_selected.split(',')
   selected_banners = [x.strip().lower() for x in selected_banners]
   # Filtering out values that start with "business"
   selected_banners = [x for x in selected_banners if not x.startswith("business")]
   if EXP_PLATFORM == 'Adobe':
       banner_filter = "AND lower(POST_EVAR4) NOT LIKE 'business%'"
       if selected_banners:
           banner_filter += " AND lower(POST_EVAR4) NOT IN (" + ", ".join(f"'{b}'" for b in selected_banners) + ")"
   elif EXP_PLATFORM == 'Decision Engine':
       banner_filter = "AND lower(BANNER_NM) NOT LIKE 'business%'"
       if selected_banners:
           banner_filter += " AND lower(BANNER_NM) NOT IN (" + ", ".join(f"'{b}'" for b in selected_banners) + ")"
else:
   banner_filter = ""
print(banner_filter)
print(banner_selected)

# COMMAND ----------

# DBTITLE 1,OS Platform Filter
if OS_PLATFORM != 'None':
    if EXP_PLATFORM == 'Adobe':
        os_platform_filter = []
        if 'iOS' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(POST_EVAR90) LIKE '%app%' AND LOWER(POST_EVAR116) LIKE '%ios%')")
        if 'Android' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(POST_EVAR90) LIKE '%app%' AND LOWER(POST_EVAR116) LIKE '%android%')")
        if 'Web' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(POST_EVAR90) LIKE '%web%')")
        if os_platform_filter:
            os_platform_filter = "AND (" + " OR ".join(os_platform_filter) + ")"
        else:
            os_platform_filter = ""
    elif EXP_PLATFORM == 'Decision Engine':
        os_platform_filter = []
        if 'iOS' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(APP_VERSION_CD) LIKE '%appios%')")
        if 'Android' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(APP_VERSION_CD) LIKE '%appand%')")
        if 'Web' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(APP_VERSION_CD) LIKE '%web%' OR APP_VERSION_CD IS NULL)")
        if os_platform_filter:
            os_platform_filter = "AND (" + " OR ".join(os_platform_filter) + ")"
        else:
            os_platform_filter = ""
    
    elif EXP_PLATFORM == 'Household IDs':
        os_platform_filter = []
        if 'iOS' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(USER_OS_TYPE_CD) LIKE 'ios_app')")
        if 'Android' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(USER_OS_TYPE_CD) LIKE 'android_app')")
        if 'Web' in OS_PLATFORM:
            os_platform_filter.append("(LOWER(USER_OS_TYPE_CD) LIKE ANY ('%web', 'desktop%'))")
        if os_platform_filter:
            os_platform_filter = "AND (" + " OR ".join(os_platform_filter) + ")"
        else:
            os_platform_filter = ""
           
    else:
        os_platform_filter = ""
                   
else:
    os_platform_filter = ""

print(os_platform_filter)
print(OS_PLATFORM)


# COMMAND ----------

# DBTITLE 1,3P HouseHold filter
household_3p_filter = f''' (safe_cast(HOUSEHOLD_ID as INT) not in (select HOUSEHOLD_ID from gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.EXP_COE_3P_HHS_LIST) OR HOUSEHOLD_ID is NULL) '''

# COMMAND ----------

# DBTITLE 1,Winsorize SQL
if WINSORIZE == '99th':
  ## If 99th percentile is equal to $0, then winsorization is ignored.
  metric_rpc_sql = '''CASE WHEN (SELECT TOT_REVENUE_WIN99 FROM WINZ) > 0 AND TOT_REVENUE > (SELECT TOT_REVENUE_WIN99 FROM WINZ) THEN (SELECT TOT_REVENUE_WIN99 FROM WINZ) ELSE TOT_REVENUE END'''
  metric_nspc_sql = '''CASE WHEN (SELECT TOT_NET_SALES_WIN99 FROM WINZ) > 0 AND TOT_NET_SALES > (SELECT TOT_NET_SALES_WIN99 FROM WINZ) THEN (SELECT TOT_NET_SALES_WIN99 FROM WINZ) ELSE TOT_NET_SALES END'''
else:
  metric_rpc_sql = 'TOT_REVENUE'
  metric_nspc_sql = 'TOT_NET_SALES'

# COMMAND ----------

# DBTITLE 1,Exposure  Check
try:
  exposure_sp_check = spark.sql("SELECT MIN(DATE(EXPOSURE_DATETIME)) as FIRST_EXPOSED_DATE, MAX(DATE(EXPOSURE_DATETIME)) as LAST_EXPOSED_DATE FROM experimentation.exposure_sp_{}".format(EXPERIMENT_ID))
  display(exposure_sp_check)
  
except:
  print('No exposure table available.')

# COMMAND ----------

# DBTITLE 1,DBFS Check
if METRIC_QUERIES == 'Standard':
  standard_sp_check = spark.sql("SELECT MIN(DATE(TXN_DTE)) as FIRST_DATE, MAX(DATE(TXN_DTE)) as LAST_DATE FROM db_work.EXP_COE_COMBINED_TXNS_GCP")
  standard_check = standard_sp_check.select("*").toPandas()

  if (pd.to_datetime(EXP_START_DATE) < pd.to_datetime(standard_check['FIRST_DATE'][0])) or (pd.to_datetime(EXP_END_DATE) > pd.to_datetime(standard_check['LAST_DATE'][0])):
      displayHTML(f"""
                  <h3><font color="red"> EXPERIMENT RUN DATES EXCEED STANDARD METRIC DATA PIPELINE! THE STANDARD PIPELINE AGGREGATES THE PREVIOUS 2 MONTHS OF DATA, IF THE EXPERIMENT RUN DATES EXCEED THIS TIME FRAME, THEN USE A CUSTOM TIME FRAME OR EXISTING DBFS LINK. </font></h3> 
                  <p><font color="black"> First Metric Date:  {standard_check['FIRST_DATE'][0]} </font></p>
                  <p><font color="black"> Last Metric Date:  {standard_check['LAST_DATE'][0]} </font></p>
                  """)
  else:
      displayHTML(f"""
                  <h3><font color="green"> EXPERIMENT RUN DATES ARE WITHIN STANDARD METRIC DATA DATES :) </font></h3> 
                  <p><font color="black"> First Date:  {standard_check['FIRST_DATE'][0]} </font></p>
                  <p><font color="black"> Last Date:  {standard_check['LAST_DATE'][0]} </font></p>
                  """)

elif METRIC_QUERIES == 'DBFS Link':
  dbfs_sp_check = spark.sql("SELECT MIN(DATE(TXN_DTE)) as FIRST_DBFS_DATE, MAX(DATE(TXN_DTE)) as LAST_DBFS_DATE FROM db_work.combined_txns_sp_r1_{}".format(DBFS_LINK))
  dbfs_check = dbfs_sp_check.select("*").toPandas()

  if (pd.to_datetime(EXP_START_DATE) < pd.to_datetime(dbfs_check['FIRST_DBFS_DATE'][0])) or (pd.to_datetime(EXP_END_DATE) > pd.to_datetime(dbfs_check['LAST_DBFS_DATE'][0])):
    displayHTML(f"""
                  <h3><font color="red"> EXPERIMENT RUN DATES EXCEED DBFS LINK! CHECK DATES AND DBFS LINK AND RE-RUN! </font></h3> 
                  <p><font color="black"> First DBFS Date:  {dbfs_check['FIRST_DBFS_DATE'][0]} </font></p>
                  <p><font color="black"> Last DBFS Date:  {dbfs_check['LAST_DBFS_DATE'][0]} </font></p>
                  """)
  else:
    displayHTML(f"""
                  <h3><font color="green"> EXPERIMENT RUN DATES ARE WITHIN DBFS DATES :) </font></h3> 
                  <p><font color="black"> First DBFS Date:  {dbfs_check['FIRST_DBFS_DATE'][0]} </font></p>
                  <p><font color="black"> Last DBFS Date:  {dbfs_check['LAST_DBFS_DATE'][0]} </font></p>
                  """)

else:
  displayHTML("""
                <h3><font color="black"> CUSTOM METRIC QUERY USED. </font></h3>
                <h3><font color="orange"> !WARNING! May take longer to run the notebook with this setting.  For shorter runs, please use the Standard or the DBFS options for the Metric Query parameter. </font></h3>   
                <p><font color="black"> Experiment_ID: {} </font></p> """.format(EXPERIMENT_ID))

# COMMAND ----------

# DBTITLE 1,Metric Query Table Logic
### If a DBFS link is provided, then the notebook will point to those tables.
### If metric queries are set to "Custom" then this notebook will kick off a worfkflow for metrics during the time of the experiment and save them into a DBFS location.
### If "Standard", then the notebook will access data from the standard metric workflows that execute daily.

if METRIC_QUERIES != 'Standard':
  if METRIC_QUERIES == 'DBFS Link':
    table_suffix = DBFS_LINK
    
  elif METRIC_QUERIES == 'Custom':
    table_suffix = EXPERIMENT_ID
  
  visit_order_table = f"""db_work.visit_order_sp_{table_suffix}"""
  cart_coupon_table = f"""db_work.cart_coupon_sp_{table_suffix}"""
  margin_table = f"""db_work.margin_sp_{table_suffix}"""
  agp_table = f"""db_work.agp_sp_{table_suffix}"""
  combined_txn_table = f"""db_work.combined_txns_sp_r1_{table_suffix}"""
  redemptions_table = f"""db_work.redemptions_sp_r1_{table_suffix}"""
  clips_table = f"""db_work.clips_sp_{table_suffix}"""
  gas_table = f"""db_work.gas_txns_sp_{table_suffix}"""
  bnc_table = f"""db_work.bnc_sp_{table_suffix}"""
  email_push_table = f"""db_work.email_push_sp_{table_suffix}"""
  basket_health_table = f"""db_work.basket_health_sp_{table_suffix}"""
  category_table = f"""db_work.category_sp_{table_suffix}"""
  account_health_table = f"""db_work.account_health_sp_{table_suffix}"""

else:
  visit_order_table = "db_work.EXP_COE_VISIT_ORDER_GCP"
  cart_coupon_table = "db_work.EXP_COE_CART_COUPON_GCP"
  margin_table = "db_work.EXP_COE_MARGIN_GCP"
  agp_table = "db_work.EXP_COE_AGP_GCP"
  combined_txn_table = "db_work.EXP_COE_COMBINED_TXNS_GCP"
  redemptions_table = "db_work.EXP_COE_REDEMPTIONS_GCP"
  clips_table = "db_work.EXP_COE_CLIPS_GCP"
  gas_table = "db_work.EXP_COE_GAS_TXNS_GCP"
  bnc_table = "db_work.EXP_COE_BNC_GCP"
  email_push_table = "db_work.EXP_COE_EMAIL_PUSH_OPT_GCP"
  basket_health_table = "db_work.EXP_COE_BASKET_HEALTH_GCP"
  category_table = "db_work.EXP_COE_CATEGORY_TXNS_GCP"
  account_health_table = "db_work.EXP_COE_ACC_HEALTH_GCP"

# COMMAND ----------

# DBTITLE 1,STOP Notebook if DBFS does not match Run Dates
if METRIC_QUERIES == 'Standard':
   if (pd.to_datetime(EXP_START_DATE) < pd.to_datetime(standard_check['FIRST_DATE'][0])) or (pd.to_datetime(EXP_END_DATE) > pd.to_datetime(standard_check['LAST_DATE'][0])):
    dbutils.notebook.exit("Experiment Run dates exceed Standard Metric Data dates. Further tasks will be skipped")
elif METRIC_QUERIES == 'DBFS Link':
  if (pd.to_datetime(EXP_START_DATE) < pd.to_datetime(dbfs_check['FIRST_DBFS_DATE'][0])) or (pd.to_datetime(EXP_END_DATE) > pd.to_datetime(dbfs_check['LAST_DBFS_DATE'][0])):
    dbutils.notebook.exit("Experiment Run dates exceed DBFS Link. Further tasks will be skipped")

# COMMAND ----------

# DBTITLE 1,CUSTOM Metric Flow
if METRIC_QUERIES == 'Custom':
  dbutils.notebook.run(
    "/Workspace/Projects/Experimentation/aaml-experimentation-coe-dev/Workflows/EXP COE - Metric Workflow", 
    0, 
    {
      "01. Start Date": EXP_START_DATE,
      "02. End Date": EXP_END_DATE,
      "03. Experiment ID": EXPERIMENT_ID, 
      "04. Metric Selection": "SAFE"
    }
  )

# COMMAND ----------

# MAGIC %md
# MAGIC # Exposures

# COMMAND ----------

# DBTITLE 1,Exposure Query
if RUN_EXPOSURE == 'True':
  if EXP_PLATFORM == 'Adobe':
    exposure_query = (
        f"""
        with UNFILTERED_EXPOSURE as(
        SELECT
            CONCAT(post_visid_high, post_visid_low) AS ID,
            post_evar49 AS ADOBE_VISITOR_ID,
            SAFE_CAST(post_evar46 AS INT) AS CLUBCARD_ID,
            FIRST_VALUE (SAFE_CAST(post_evar47 AS INT) IGNORE NULLS) OVER (PARTITION BY CONCAT(post_visid_high, post_visid_low) ORDER BY DATE_TIME ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS HOUSEHOLD_ID,
            {control_strategy},
            DATE(DATE_TIME) AS EXPOSURE_DATETIME
          FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CLICK_HIT_DATA
          WHERE 1=1
            AND DATE(DW_CREATETS) > DATE_ADD(DATE('{EXP_START_DATE}'), INTERVAL -1 DAY)
            AND (DATE(DATE_TIME) >= '{EXP_START_DATE}' AND DATE(DATE_TIME) <= '{EXP_END_DATE}')
            AND POST_TNT LIKE '%{EXPERIMENT_ID}%'
            AND CAST(EXCLUDE_HIT AS INT) = 0
            AND CAST(hit_source AS INT) NOT IN (5,7,8,9)
            {page_filter}
            {os_platform_filter}
            {banner_filter}
          QUALIFY ROW_NUMBER() OVER(PARTITION BY ADOBE_VISITOR_ID ORDER BY DATE_TIME ASC) = 1
          )

          select *
          from UNFILTERED_EXPOSURE
          WHERE {household_3p_filter}
          """
        )
    
  elif EXP_PLATFORM == 'Decision Engine':
    exposure_query = (
        f"""
        SELECT 
            HOUSEHOLD_ID
          , CASE
              WHEN DECISION_ENGINE_VARIANT_CD = 'A' THEN 'VARIANT A'
              WHEN DECISION_ENGINE_VARIANT_CD = 'B' THEN 'VARIANT B'
              WHEN DECISION_ENGINE_VARIANT_CD = 'C' THEN 'VARIANT C'
              WHEN DECISION_ENGINE_VARIANT_CD = 'D' THEN 'VARIANT D'
              WHEN DECISION_ENGINE_VARIANT_CD = 'E' THEN 'VARIANT E'
              WHEN DECISION_ENGINE_VARIANT_CD = 'F' THEN 'VARIANT F'
              WHEN DECISION_ENGINE_VARIANT_CD = 'G' THEN 'VARIANT G'
            ELSE NULL
          END AS VARIANT_ID
          , DATE(TIMESTAMP(EVENT_TS,'America/Boise')) AS EXPOSURE_DATETIME
        FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CUSTOMER_SESSION_EVENT_MASTER
        WHERE DECISION_ENGINE_EXPERIMENT_ID LIKE '%{EXPERIMENT_ID}%'
          AND DATE(TIMESTAMP(EVENT_TS,'America/Boise')) BETWEEN  '{EXP_START_DATE}' and '{EXP_END_DATE}'
          AND CAST(DECISION_ENGINE_VARIANT_CD AS STRING) IN ('A','B','C','D','E','F','G')
          {page_filter}
          {os_platform_filter}
          {banner_filter}
          AND {household_3p_filter}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY DECISION_ENGINE_EXPERIMENT_ID,HOUSEHOLD_ID,VARIANT_ID ORDER BY EVENT_TS)=1
        """
      )
    
  elif ((EXP_PLATFORM == 'Push') | (EXP_PLATFORM == 'Email')):
    exposure_query = (
      """
        #### TODO: NEED TO UPDATE THIS
      """
      )
  
  elif EXP_PLATFORM == 'Household IDs':

    # FACTS Column check
    column_check_query = f'''
      SELECT column_name
      FROM gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.INFORMATION_SCHEMA.COLUMNS
      WHERE table_name = '{EXPERIMENT_ID}'
      '''
    column_name_list = bc.read_gcp_table(column_check_query).select("column_name").rdd.flatMap(lambda x: x).collect()
    if 'ANNUAL_FACTS' in column_name_list:
      FACTS_COLUMN_PULL = 'ANNUAL_FACTS'
    elif 'TWELVE_WEEKS_FACTS' in column_name_list:
      FACTS_COLUMN_PULL = 'TWELVE_WEEKS_FACTS'
    else:
      FACTS_COLUMN_PULL = 'FACTS'
    
    # Exposure Query
    exposure_query = (
      f"""
      SELECT DISTINCT
        SAFE_CAST(HOUSEHOLD_ID AS INT) as HOUSEHOLD_ID
        , CASE 
            WHEN VARIANT = 'A' THEN 'VARIANT A'
            WHEN VARIANT = 'B' THEN 'VARIANT B'
            WHEN VARIANT = 'C' THEN 'VARIANT C'
            WHEN VARIANT = 'D' THEN 'VARIANT D'
            WHEN VARIANT = 'E' THEN 'VARIANT E'
            WHEN VARIANT = 'F' THEN 'VARIANT F'
            WHEN VARIANT = 'G' THEN 'VARIANT G'
            WHEN VARIANT = 'H' THEN 'VARIANT H'
            WHEN VARIANT = 'I' THEN 'VARIANT I'
            ELSE NULL
          END AS VARIANT_ID,
          {FACTS_COLUMN_PULL},
          MYNEEDS,
          {SEGMENTATION}
          CAST('{EXP_START_DATE}' AS DATE) AS EXPOSURE_DATETIME
      FROM gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.{EXPERIMENT_ID}
      where {household_3p_filter}
      """
      )

  print(exposure_query)
  # exposure_sp = bc.read_gcp_table(exposure_query)
  # exposure_sp.createOrReplaceTempView('exposure_sp')

  exposure_sp_raw = bc.read_gcp_table(exposure_query)
  exposure_sp_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'''experimentation.exposure_sp_raw_{EXPERIMENT_ID}''')  
  
  exposure_sp = spark.table(f'''experimentation.exposure_sp_raw_{EXPERIMENT_ID}''')
  exposure_sp.createOrReplaceTempView('exposure_sp')

# COMMAND ----------

# DBTITLE 1,SRM By Date
from pyspark.sql import Window

if EXP_PLATFORM != 'Household IDs':
  distinct_counts = (exposure_sp.groupBy('VARIANT_ID', 'EXPOSURE_DATETIME').agg(F.countDistinct(visitor_unit).alias('distinct_visitors')))
  windowval = (Window.partitionBy('VARIANT_ID').orderBy('EXPOSURE_DATETIME').rangeBetween(Window.unboundedPreceding, 0))

  exposure_rolling = (distinct_counts
                      .withColumn('visitors_rolling', F.sum('distinct_visitors').over(windowval)))
  
  exposure_control = (exposure_rolling.filter(F.col('VARIANT_ID') == 'VARIANT A')
                      .select('EXPOSURE_DATETIME','distinct_visitors','visitors_rolling')
                      .withColumnRenamed('distinct_visitors','distinct_visitors_control')
                      .withColumnRenamed('visitors_rolling','visitors_rolling_control'))
  
  exposure_rolling = exposure_rolling.join(exposure_control, on='EXPOSURE_DATETIME', how='left')
  exposure_rolling = (exposure_rolling
                      .withColumn('distinct_visitors_diff', F.col('distinct_visitors') - F.col('distinct_visitors_control'))
                      .withColumn('visitors_rolling_diff', F.col('visitors_rolling') - F.col('visitors_rolling_control')))


  exposure_rolling_df = exposure_rolling.toPandas()
  if exposure_rolling_df['VARIANT_ID'].unique().shape[0] <= 2:
    exposure_diff = exposure_rolling_df[exposure_rolling_df['VARIANT_ID'] != 'VARIANT A'][['EXPOSURE_DATETIME','distinct_visitors_diff', 'visitors_rolling_diff']]
  else:
    exposure_diff = exposure_rolling_df[exposure_rolling_df['VARIANT_ID'] != 'VARIANT A'][['VARIANT_ID','EXPOSURE_DATETIME','distinct_visitors_diff', 'visitors_rolling_diff']]

  for i in list(exposure_rolling_df['EXPOSURE_DATETIME'].unique()):
    visitor_counts = exposure_rolling_df.loc[exposure_rolling_df['EXPOSURE_DATETIME'] == i, 'visitors_rolling']
    exposure_diff.loc[exposure_diff['EXPOSURE_DATETIME'] == i, 'ADOBE_SRM p-value'] = round(srm_test(visitor_counts,exp_platform = EXP_PLATFORM)[1],4)

  display(exposure_diff.sort_values('EXPOSURE_DATETIME'))

# COMMAND ----------

# DBTITLE 1,Raw SRM Check
if EXP_PLATFORM == 'Adobe':
  srm_check_raw = exposure_sp.groupBy('VARIANT_ID').agg(
                        F.countDistinct('HOUSEHOLD_ID').alias("HH_COUNT"),
                        F.countDistinct('ADOBE_VISITOR_ID').alias("Adobe_Count")).sort('VARIANT_ID').toPandas()

  display(srm_check_raw)

  print('Adobe SRM p-value:', round(srm_test(srm_check_raw['Adobe_Count'],exp_platform = EXP_PLATFORM)[1],4))
  print('HH SRM p-value:', round(srm_test(srm_check_raw['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4))

# COMMAND ----------

# DBTITLE 1,Exposure Filter
if EXPOSURE_FILTER == 'BNC':
  filter_query = (
    f"""
    SELECT HHS_ID AS HOUSEHOLD_ID, 
          SEGMENT_1 AS SEGMENTATION 
    FROM gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.ESA_HOLISTIC_SCORECARD_TXN_CUSTOMER_SEGMENT
    WHERE TXN_DTE BETWEEN '{EXP_START_DATE}' AND '{EXP_END_DATE}'  
      AND SEGMENT_1 LIKE'BNC TO ECOMM'
    """)
  
elif EXPOSURE_FILTER == 'SNAP':
  filter_query = (
    f"""
    SELECT DISTINCT
            smv.HOUSEHOLD_ID
    FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_retl.TXN_HDR_COMBINED AS f
    JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_edw.LU_DAY_MERGE AS b
      ON CAST(f.TXN_DTE AS DATE) = b.D_DATE
    JOIN gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.LU_STOREID_DIVISION AS d
      ON f.STORE_ID = d.STORE_ID
    JOIN (SELECT DISTINCT HOUSEHOLD_ID, LOYALTY_PROGRAM_CARD_NBR FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.SMV_RETAIL_CUSTOMER_LOYALTY_PROGRAM_HOUSEHOLD) as smv
      ON f.CARD_NBR = smv.LOYALTY_PROGRAM_CARD_NBR
    WHERE 1=1 
      AND (f.TXN_DTE >= '{EXP_START_DATE}' AND f.TXN_DTE <= '{EXP_END_DATE}')
      AND f.TXN_HDR_SRC_CD = 0
      AND f.REGISTER_NBR IN (99,173,174,999,16,17,18,19,20,49,50,51,52,53,54,93,94,95,96,97,98,151,152,153,154,175,176,177,178,179,180,181,182,195,196,197,198,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,116,117,118,119,120,121,122,123,124,125) 
      AND (f.TENDER_AMT_FOODSTAMPS + f.TENDER_AMT_EBT) > 0
      """)

elif EXPOSURE_FILTER == 'Page or OS platform':
  filter_query = f"""
    SELECT DISTINCT SAFE_CAST(RETAIL_CUSTOMER_HHID_TXT AS INT) AS HOUSEHOLD_ID
    FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_cust.CLICK_STREAM_VISIT_VIEW
    WHERE DATE(TIMESTAMP(visit_start_ts),'America/Denver') BETWEEN '{EXP_START_DATE}' AND '{EXP_END_DATE}'
    {page_filter}
    {os_platform_filter}
    """

elif EXPOSURE_FILTER == 'Custom':
  filter_query = (
    """
    INPUT FILTER QUERY HERE
    """
  )  

if EXPOSURE_FILTER != 'None':

  print(filter_query)
  filter_sp = bc.read_gcp_table(filter_query)

  exposure_filter_sp = exposure_sp.join(filter_sp,on='HOUSEHOLD_ID',how='inner')
  exposure_filter_sp.createOrReplaceTempView("exposure_filter")
  exposure_table = "exposure_filter"
  exposure_filter_sp.display(10)

else:
  exposure_table = 'exposure_sp'
  # exposure_table = 'experimentation.EXPOSURE_SP_{}'.format(EXPERIMENT_ID)

print('Exposure Table: {}'.format(exposure_table))

# COMMAND ----------

# DBTITLE 1,Remove HHs that see 2 Variants but Include Null HH_IDs
exposure_deduped_sp = spark.sql(f"""
WITH 
HH_DEDUPED as(
SELECT HOUSEHOLD_ID 
from {exposure_table}
GROUP BY HOUSEHOLD_ID HAVING count(distinct VARIANT_ID) = 1
)

SELECT * 
from {exposure_table}
WHERE HOUSEHOLD_ID IS NULL
OR HOUSEHOLD_ID IN (SELECT * FROM HH_DEDUPED) 
""")

exposure_table = 'exposure_deduped_with_nulls'
exposure_deduped_sp.createOrReplaceTempView(f"{exposure_table}")

# COMMAND ----------

# DBTITLE 1,DeDuped SRM Check
if EXP_PLATFORM == 'Adobe':
  srm_check_deduped = exposure_deduped_sp.groupBy('VARIANT_ID').agg(
                        F.countDistinct('HOUSEHOLD_ID').alias("HH_COUNT"),
                        F.countDistinct('ADOBE_VISITOR_ID').alias("ADOBE_VISITORS"),
                        F.countDistinct(F.when(F.col('HOUSEHOLD_ID').isNull(), F.col('ADOBE_VISITOR_ID')).otherwise(None)).alias("ADOBE_VISITORS_wNULL_HH"),
                        F.countDistinct(F.when(F.col('HOUSEHOLD_ID').isNotNull(), F.col('ADOBE_VISITOR_ID')).otherwise(None)).alias("ADOBE_VISITORS_noNULL_HH")).sort('VARIANT_ID').toPandas()

  srm_check_deduped['VISITOR_COUNT'] = srm_check_deduped['ADOBE_VISITORS_wNULL_HH'] + srm_check_deduped['HH_COUNT']

  display(srm_check_deduped)

  print('Adobe SRM p-value:', round(srm_test(srm_check_deduped['ADOBE_VISITORS'],exp_platform = EXP_PLATFORM)[1],4))
  print('Adobe wNull SRM p-value:', round(srm_test(srm_check_deduped['ADOBE_VISITORS_wNULL_HH'],exp_platform = EXP_PLATFORM)[1],4))
  print('HH SRM p-value:', round(srm_test(srm_check_deduped['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4))

# COMMAND ----------

# DBTITLE 1,Exposure Split by HH and Adobe Visitor
#### For Adobe tests, break exposure table into visitors with household_ids and those without. ####
if EXP_PLATFORM == 'Adobe':
  exposure_split = (spark.sql(f"""
  WITH
  -- Ensure that we join on only 1 HOUSEHOLD_ID
  EXPOSURE_HH AS(
  SELECT HOUSEHOLD_ID
      , ID
      , ADOBE_VISITOR_ID
      , VARIANT_ID
      , EXPOSURE_DATETIME
  FROM {exposure_table}
  WHERE HOUSEHOLD_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID ORDER BY EXPOSURE_DATETIME ASC) = 1
  ),

  -- GRAB ADOBE_VISITOR_IDs with NULL HOUSEHOLD_ID
  EXPOSURE_VISITORS AS(
  SELECT HOUSEHOLD_ID
      , ID
      , ADOBE_VISITOR_ID
      , VARIANT_ID
      , EXPOSURE_DATETIME
  FROM {exposure_table}
  WHERE HOUSEHOLD_ID IS NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ADOBE_VISITOR_ID ORDER BY EXPOSURE_DATETIME ASC) = 1
  )

  -- UNION to get overall exposure.
  SELECT * FROM EXPOSURE_HH
  UNION DISTINCT SELECT * FROM EXPOSURE_VISITORS
  """))
else:
  
  exposure_split = (spark.sql(f"""
  -- Ensure that we join on only 1 HOUSEHOLD_ID
  SELECT *
  FROM {exposure_table}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY HOUSEHOLD_ID ORDER BY EXPOSURE_DATETIME ASC) = 1
  """))


exposure_split.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'''experimentation.exposure_sp_{EXPERIMENT_ID}''')
exposure_split_sp = spark.table(f"experimentation.exposure_sp_{EXPERIMENT_ID}")

exposure_table = 'exposure_split_sp'
exposure_split_sp.createOrReplaceTempView(f"{exposure_table}")

# COMMAND ----------

# DBTITLE 1,Final SRM Check
if EXP_PLATFORM == 'Adobe':
  srm_check_final = exposure_split_sp.groupBy('VARIANT_ID').agg(
                        F.countDistinct('HOUSEHOLD_ID').alias("HH_COUNT"),
                        F.countDistinct('ADOBE_VISITOR_ID').alias("ADOBE_VISITORS"),
                        F.countDistinct(F.when(F.col('HOUSEHOLD_ID').isNull(), F.col('ADOBE_VISITOR_ID')).otherwise(None)).alias("ADOBE_VISITORS_wNULL_HH"),
                        F.countDistinct(F.when(F.col('HOUSEHOLD_ID').isNotNull(), F.col('ADOBE_VISITOR_ID')).otherwise(None)).alias("ADOBE_VISITORS_noNULL_HH")).sort('VARIANT_ID').toPandas()

  srm_check_final['VISITOR_COUNT'] = srm_check_final['ADOBE_VISITORS_wNULL_HH'] + srm_check_final['HH_COUNT']

  display(srm_check_final)

  print('VISITOR SRM p-value:', round(srm_test(srm_check_final['ADOBE_VISITORS'],exp_platform = EXP_PLATFORM)[1],4))
  print('Adobe wNull SRM p-value:', round(srm_test(srm_check_final['ADOBE_VISITORS_wNULL_HH'],exp_platform = EXP_PLATFORM)[1],4))
  print('HH SRM p-value:', round(srm_test(srm_check_final['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4))

else:
  srm_check_final = exposure_split_sp.groupBy('VARIANT_ID').agg(
                        F.countDistinct('HOUSEHOLD_ID').alias("HH_COUNT")).sort('VARIANT_ID').toPandas()

  display(srm_check_final)

  print('HH SRM p-value:', round(srm_test(srm_check_final['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4))

# COMMAND ----------

# DBTITLE 1,SRM Display
if EXP_PLATFORM == 'Adobe':
  if round(srm_test(srm_check_raw['Adobe_Count'],exp_platform = EXP_PLATFORM)[1],4) < 0.01:
    srm_message_raw = f"""<h3><font color="red"> SAMPLE RATIO MISMATCH FOUND! p-value < 0.01 for Raw Adobe_Visitor_ID's </font></h3>"""
  else:
    srm_message_raw = f"""<h3><font color="green"> No Sample Ratio Mismatch Found for Raw Adobe_Visitor_ID's </font></h3>"""
  
  if round(srm_test(srm_check_final['ADOBE_VISITORS'],exp_platform = EXP_PLATFORM)[1],4) < 0.01:
    srm_message_adobe = f"""<h3><font color="red"> SAMPLE RATIO MISMATCH FOUND! p-value < 0.01 for Deduped Adobe_Visitor_ID's </font></h3>"""
  else:
    srm_message_adobe = f"""<h3><font color="green"> No Sample Ratio Mismatch Found for Deduped Adobe_Visitor_ID's </font></h3>"""

  if round(srm_test(srm_check_final['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4) < 0.01:
    srm_message_hh = f"""<h3><font color="red"> SAMPLE RATIO MISMATCH FOUND! p-value < 0.01 for Deduped Household_ID's </font></h3>"""
  else:
    srm_message_hh = f"""<h3><font color="green"> No Sample Ratio Mismatch Found for Deduped Household_ID's </font></h3>"""

else:
  srm_message_raw = ""
  srm_message_adobe = ""
  if round(srm_test(srm_check_final['HH_COUNT'],exp_platform = EXP_PLATFORM)[1],4) < 0.01:
    srm_message_hh = f"""<h3><font color="red"> SAMPLE RATIO MISMATCH FOUND! p-value < 0.01 for Deduped Household_ID's </font></h3>"""
  else:
    srm_message_hh = f"""<h3><font color="green"> No Sample Ratio Mismatch Found for Deduped Household_ID's </font></h3>"""

displayHTML(f"""{srm_message_raw}
            {srm_message_adobe}
            {srm_message_hh}""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Metric Aggregation

# COMMAND ----------

# DBTITLE 1,Engagement Aggregation on Adobe_Visitor_ID
visitor_id = """ID"""
join_str = """CAST(e.ID AS STRING) = CAST(v.ID AS STRING)"""

agg_daily_AV_query = (f"""
    WITH
    EXPOSURE_VISITORS AS(
      SELECT * FROM {exposure_table} WHERE HOUSEHOLD_ID IS NULL
    ),

    VISITS AS(
      SELECT
        VISIT_ID,
        {visitor_id},
        VISIT_START_TS,
        VISIT_END_TS,
        SUM(NUM_ORDERS) AS NUM_ORDERS,
        SUM(TOT_REVENUE) AS TOT_REVENUE,
        SUM(NUM_UNITS) AS NUM_UNITS
      FROM
        {visit_order_table}
      WHERE 1=1
          AND (
                (DATE(visit_start_ts) >= '{EXP_START_DATE}' AND DATE(visit_start_ts) <= '{EXP_END_DATE}')
                OR 
                (DATE(visit_end_ts) >= '{EXP_START_DATE}' AND DATE(visit_end_ts) <= '{EXP_END_DATE}')
              )
      GROUP BY 1,2,3,4
      ),

    EXPOSURES_VISITS AS(   
      SELECT
          e.*,
          COALESCE(COUNT(DISTINCT v.VISIT_ID),0) AS NUM_VISITS,
          COALESCE(SUM(NUM_ORDERS),0) AS NUM_ORDERS,
          COALESCE(SUM(TOT_REVENUE),0) AS TOT_REVENUE,
          COALESCE(SUM(NUM_UNITS),0) AS NUM_UNITS
      FROM EXPOSURE_VISITORS AS e
      LEFT JOIN VISITS as v
          ON {join_str}
          AND v.VISIT_END_TS >= e.EXPOSURE_DATETIME
      WHERE e.VARIANT_ID IS NOT NULL        
      GROUP BY all
      ),

    CART_ADDS AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS CART_ADDS
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'CART_ADD'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY all
    ),

    COUPON_CLIPS AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS COUPON_CLIPS
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'COUPON_CLIP'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY all
    ),

    SEARCHES AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS NUM_SEARCHES
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'SEARCH'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY all
    ),

    EXPOSURES_VISITS_CART_ADDS AS(
      SELECT
        ev.*, 
        COALESCE(SUM(ca.CART_ADDS),0) AS NUM_CART_ADDS
      FROM EXPOSURES_VISITS AS ev
      LEFT JOIN CART_ADDS AS ca
        ON ev.{visitor_id} = ca.{visitor_id}
        AND TIMESTAMP(ev.EXPOSURE_DATETIME) <= TIMESTAMP(ca.DTE)
      GROUP BY all
      ),

      EXPOSURES_VISITS_COUPON_CLIPS AS(
        SELECT
          evca.*,
          COALESCE(SUM(cc.COUPON_CLIPS),0) AS COUPON_CLIPS
        FROM  EXPOSURES_VISITS_CART_ADDS evca
        LEFT JOIN COUPON_CLIPS AS cc
          ON evca.{visitor_id} = cc.{visitor_id}
          AND TIMESTAMP(evca.EXPOSURE_DATETIME) <= TIMESTAMP(cc.DTE)
        GROUP BY all
        )
        
        SELECT
          evcc.*,
          COALESCE(SUM(ss.NUM_SEARCHES),0) AS NUM_SEARCHES
        FROM  EXPOSURES_VISITS_COUPON_CLIPS evcc
        LEFT JOIN SEARCHES AS ss
          ON evcc.{visitor_id} = ss.{visitor_id}
          AND TIMESTAMP(evcc.EXPOSURE_DATETIME) <= TIMESTAMP(ss.DTE)
        GROUP BY all
""")

if EXP_PLATFORM == 'Adobe':
  agg_daily_AV = spark.sql(agg_daily_AV_query)
  agg_daily_AV.createOrReplaceTempView("engagement_agg_AV")

# COMMAND ----------

# DBTITLE 1,Engagement Aggregation on HOUSEHOLD_ID
visitor_id = """HOUSEHOLD_ID"""
join_str = """e.HOUSEHOLD_ID = v.HOUSEHOLD_ID"""

agg_daily_HH_query = (f"""
    WITH
    EXPOSURE_HH AS(
      SELECT * FROM {exposure_table} WHERE HOUSEHOLD_ID IS NOT NULL
    ),
    
    VISITS AS(
      SELECT
        VISIT_ID,
        {visitor_id},
        VISIT_START_TS,
        VISIT_END_TS,
        SUM(NUM_ORDERS) AS NUM_ORDERS,
        SUM(TOT_REVENUE) AS TOT_REVENUE,
        SUM(NUM_UNITS) AS NUM_UNITS
      FROM
        {visit_order_table}
      WHERE 1=1
          AND (
                (DATE(visit_start_ts) >= '{EXP_START_DATE}' AND DATE(visit_start_ts) <= '{EXP_END_DATE}')
                OR 
                (DATE(visit_end_ts) >= '{EXP_START_DATE}' AND DATE(visit_end_ts) <= '{EXP_END_DATE}')
              )
      GROUP BY 1,2,3,4
      ),

    EXPOSURES_VISITS AS(   
      SELECT
          e.*,
          COALESCE(COUNT(DISTINCT v.VISIT_ID),0) AS NUM_VISITS,
          COALESCE(SUM(NUM_ORDERS),0) AS NUM_ORDERS,
          COALESCE(SUM(TOT_REVENUE),0) AS TOT_REVENUE,
          COALESCE(SUM(NUM_UNITS),0) AS NUM_UNITS
      FROM EXPOSURE_HH AS e
      LEFT JOIN VISITS as v
          ON {join_str}
          AND v.VISIT_END_TS >= e.EXPOSURE_DATETIME
      WHERE e.VARIANT_ID IS NOT NULL        
      GROUP BY all
      ),

    CART_ADDS AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS CART_ADDS
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'CART_ADD'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY 1,2
    ),

    COUPON_CLIPS AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS COUPON_CLIPS
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'COUPON_CLIP'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY 1,2
    ),

    SEARCHES AS(
      SELECT 
        {visitor_id}, 
        DTE,
        COUNT(*) AS NUM_SEARCHES
      FROM {cart_coupon_table}
      WHERE EVENT_TYPE = 'SEARCH'
      AND (DATE(DTE) >= '{EXP_START_DATE}' AND DATE(DTE) <= '{EXP_END_DATE}')
      GROUP BY 1,2
    ),

    EXPOSURES_VISITS_CART_ADDS AS(
      SELECT
        ev.*,
        COALESCE(SUM(ca.CART_ADDS),0) AS NUM_CART_ADDS
      FROM EXPOSURES_VISITS AS ev
      LEFT JOIN CART_ADDS AS ca
        ON ev.{visitor_id} = ca.{visitor_id}
        AND TIMESTAMP(ev.EXPOSURE_DATETIME) <= TIMESTAMP(ca.DTE)
      GROUP BY all
      ),

      EXPOSURES_VISITS_COUPON_CLIPS AS(
        SELECT
          evca.*,
          COALESCE(SUM(cc.COUPON_CLIPS),0) AS COUPON_CLIPS
        FROM  EXPOSURES_VISITS_CART_ADDS evca
        LEFT JOIN COUPON_CLIPS AS cc
          ON evca.{visitor_id} = cc.{visitor_id}
          AND TIMESTAMP(evca.EXPOSURE_DATETIME) <= TIMESTAMP(cc.DTE)
        GROUP BY all
        )

        SELECT
          evcc.*,
          COALESCE(SUM(ss.NUM_SEARCHES),0) AS NUM_SEARCHES
        FROM  EXPOSURES_VISITS_COUPON_CLIPS evcc
        LEFT JOIN SEARCHES AS ss
          ON evcc.{visitor_id} = ss.{visitor_id}
          AND TIMESTAMP(evcc.EXPOSURE_DATETIME) <= TIMESTAMP(ss.DTE)
        GROUP BY all
          """)
  
agg_daily_HH = spark.sql(agg_daily_HH_query)
agg_daily_HH.createOrReplaceTempView("engagement_agg_HH")

# COMMAND ----------

# DBTITLE 1,Click-Hit Aggregation
if EXP_PLATFORM == 'Adobe':
  engagement_sub_query = """
  SELECT * FROM engagement_agg_AV
  UNION DISTINCT (SELECT * FROM engagement_agg_HH)
  """
else:
  engagement_sub_query = """SELECT * FROM engagement_agg_HH"""

engagement_agg_query = (f"""
WITH
COMBINED_ENGAGEMENT_AGG AS(
{engagement_sub_query}
)

SELECT
          VARIANT_ID,
          COUNT(DISTINCT {visitor_unit}) AS VISITORS,
          COUNT(DISTINCT HOUSEHOLD_ID) AS UNIQUE_HOUSEHOLDS,
        -- VISITS
          SUM(NUM_VISITS) AS VISITS_TOTAL,
          AVG(NUM_VISITS) AS VISITS_MEAN,
          STDDEV(NUM_VISITS) AS VISITS_SD,
        -- ORDERS
          SUM(NUM_ORDERS) AS ORDERS_TOTAL,
          AVG(NUM_ORDERS) AS ORDERS_MEAN,
          STDDEV(NUM_ORDERS) AS ORDERS_SD,
          AVG(
              CASE WHEN NUM_ORDERS = 0 THEN NULL
              ELSE NUM_ORDERS
              END
          ) AS ORDERS_NONZERO_MEAN,
          STDDEV(
              CASE WHEN NUM_ORDERS = 0 THEN NULL
              ELSE NUM_ORDERS
              END
          ) AS ORDERS_NONZERO_SD,
          COUNT(DISTINCT CASE WHEN NUM_ORDERS > 0 THEN {visitor_unit} ELSE NULL END) AS UNIQUE_USERS_THAT_ORDER,
          COUNT(DISTINCT CASE WHEN NUM_ORDERS > 0 THEN {visitor_unit} ELSE NULL END) / COUNT(DISTINCT {visitor_unit}) AS CVR,
        -- UNITS
          SUM(NUM_UNITS) AS UNITS_TOTAL,
          AVG(NUM_UNITS) AS UNITS_MEAN,
          STDDEV(NUM_UNITS) AS UNITS_SD,
          AVG(
              CASE WHEN NUM_UNITS = 0 THEN NULL
              ELSE NUM_UNITS
              END
          ) AS UNITS_NONZERO_MEAN,
          STDDEV(
              CASE WHEN NUM_UNITS = 0 THEN NULL
              ELSE NUM_UNITS
              END
          ) AS UNITS_NONZERO_SD,
        -- REVENUE
          SUM(TOT_REVENUE) AS REVENUE_TOTAL,
          AVG(TOT_REVENUE) AS REVENUE_MEAN,
          STDDEV(TOT_REVENUE) AS REVENUE_SD,
          AVG(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE TOT_REVENUE END) AS REVENUE_NONZERO_MEAN,
          STDDEV(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE TOT_REVENUE END) AS REVENUE_NONZERO_SD,
        -- COUPON CLIPS
          SUM(COUPON_CLIPS) as COUPON_CLIPS_TOTAL,
          AVG(COUPON_CLIPS) AS COUPON_CLIPS_MEAN,
          STDDEV(COUPON_CLIPS) AS COUPON_CLIPS_SD,
          AVG(
              CASE WHEN COUPON_CLIPS = 0 THEN NULL
              ELSE COUPON_CLIPS
              END
          ) AS COUPON_CLIPS_NONZERO_MEAN,
          STDDEV(
              CASE WHEN COUPON_CLIPS = 0 THEN NULL
              ELSE COUPON_CLIPS
              END
            ) AS COUPON_CLIPS_NONZERO_SD,
          COUNT(DISTINCT CASE WHEN COUPON_CLIPS > 0 THEN {visitor_unit} ELSE NULL END) AS UNIQUE_USERS_THAT_COUPON_CLIP,
          COUNT(DISTINCT CASE WHEN COUPON_CLIPS > 0 THEN {visitor_unit} ELSE NULL END) / COUNT(DISTINCT {visitor_unit}) AS COUPON_CLIP_CVR,
        -- CART ADDS
          SUM(NUM_CART_ADDS) AS CART_ADDS_TOTAL,
          AVG(NUM_CART_ADDS) AS CART_ADDS_MEAN,
          STDDEV(NUM_CART_ADDS) AS CART_ADDS_SD,
          AVG(
              CASE WHEN NUM_CART_ADDS = 0 THEN NULL
              ELSE NUM_CART_ADDS
              END
          ) AS CART_ADDS_CONDITIONAL_MEAN,
          STDDEV(
              CASE WHEN NUM_CART_ADDS = 0 THEN NULL
              ELSE NUM_CART_ADDS
              END
          ) AS CART_ADDS_CONDITIONAL_SD,
          COUNT(DISTINCT CASE WHEN NUM_CART_ADDS > 0 THEN {visitor_unit} ELSE NULL END) AS UNIQUE_USERS_THAT_ADDTOCART,
          COUNT(DISTINCT CASE WHEN NUM_CART_ADDS > 0 THEN {visitor_unit} ELSE NULL END) / COUNT(DISTINCT {visitor_unit}) AS CART_ADDS_CVR,
        -- SEARCHES    
          SUM(NUM_SEARCHES) AS SEARCHES_TOTAL,
          AVG(NUM_SEARCHES) AS SEARCHES_MEAN,
          STDDEV(NUM_SEARCHES) AS SEARCHES_SD,
          AVG(
              CASE WHEN NUM_SEARCHES = 0 THEN NULL
              ELSE NUM_SEARCHES
              END
          ) AS SEARCHES_CONDITIONAL_MEAN,
          STDDEV(
              CASE WHEN NUM_SEARCHES = 0 THEN NULL
              ELSE NUM_SEARCHES
              END
          ) AS SEARCHES_CONDITIONAL_SD,
          COUNT(DISTINCT CASE WHEN NUM_SEARCHES > 0 THEN {visitor_unit} ELSE NULL END) AS UNIQUE_USERS_THAT_SEARCH,
          COUNT(DISTINCT CASE WHEN NUM_SEARCHES > 0 THEN {visitor_unit} ELSE NULL END) / COUNT(DISTINCT {visitor_unit}) AS SEARCHES_CVR,
        -- Ratio Metrics
          AVG(IFF(TOT_REVENUE > 0, TOT_REVENUE, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS AOV,
          COVAR_SAMP(TOT_REVENUE,NUM_ORDERS) AS COV_REVENUE_ORDERS,
          AVG(IFF(NUM_UNITS > 0, NUM_UNITS, NULL)) / AVG(IFF(NUM_ORDERS > 0 , NUM_ORDERS, NULL)) AS UPO,
          COVAR_SAMP(NUM_UNITS,NUM_ORDERS) AS COV_UNITS_ORDERS,
          SUM(TOT_REVENUE) / COUNT(DISTINCT {visitor_unit}) AS RPV
        FROM COMBINED_ENGAGEMENT_AGG
        GROUP BY all
        ORDER BY 1                                            
""")

agg_daily_sp = spark.sql(engagement_agg_query)
# agg_daily_sp.display()

# COMMAND ----------

# DBTITLE 1,Margin Aggregation
margin_agg_query = (
  f"""
  WITH
  EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),
  
  EXPOSURE_WEEK AS(
  SELECT e.*,
      DATEADD(DAY,-1,DATE_TRUNC('WEEK',DATEADD(DAY,1,EXPOSURE_DATETIME))) AS EXP_WEEK_START 
  FROM EXPOSURE_BASE AS e
  GROUP BY all
  ),

  BASE AS(
  SELECT e.*,
      COALESCE(sum(m.AGP_VAL_WITHOUT_INSTA_ADJ),0) as margin
  FROM EXPOSURE_WEEK as e
  LEFT JOIN {margin_table} as m
      ON e.HOUSEHOLD_ID = m.HOUSEHOLD_ID
      AND m.FISCAL_WEEK_START_DT BETWEEN e.EXP_WEEK_START AND '{EXP_END_DATE}'
    WHERE e.HOUSEHOLD_ID IS NOT NULL
  GROUP BY all
)

  SELECT
    VARIANT_ID,
    COUNT(DISTINCT {visitor_unit}) AS VISITORS,
  --- MARGIN
    SUM(margin) AS MARGIN_TOTAL,
    AVG(margin) AS MARGIN_MEAN,
    STDDEV(margin) AS MARGIN_SD,
    AVG(CASE WHEN margin = 0 THEN NULL ELSE margin END) AS MARGIN_NONZERO_MEAN,
    STDDEV(CASE WHEN margin = 0 THEN NULL ELSE margin END) AS MARGIN_NONZERO_SD
  FROM BASE
  GROUP BY all
  ORDER BY 1
""")

margin_agg_sp = spark.sql(margin_agg_query)
#margin_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,AGP Aggregation
agp_agg_query = (f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

BASE AS(
  SELECT 
      e.*, 
      COALESCE(sum(m.NET_SALES),0) as TOT_NET_SALES,
      COALESCE(sum(m.AGP_TOT),0) as AGP
  FROM EXPOSURE_BASE as e
  LEFT JOIN {agp_table} as m
      ON e.HOUSEHOLD_ID = m.HOUSEHOLD_ID
      AND m.TXN_DTE BETWEEN DATE(e.EXPOSURE_DATETIME) AND '{EXP_END_DATE}'
  GROUP BY all
),

WINZ AS(
     SELECT
       APPROX_PERCENTILE(TOT_NET_SALES,0.99) AS TOT_NET_SALES_WIN99
     FROM BASE
     WHERE HOUSEHOLD_ID IS NOT NULL
)

SELECT
  VARIANT_ID, 
  COUNT(DISTINCT {visitor_unit}) AS VISITORS,
--- NET_SALES
  SUM({metric_nspc_sql}) AS NET_SALES_TOTAL,
  AVG({metric_nspc_sql}) AS NET_SALES_MEAN,
  STDDEV({metric_nspc_sql}) AS NET_SALES_SD,
--- AGP
  SUM(AGP) AS AGP_TOTAL,
  AVG(AGP) AS AGP_MEAN,
  STDDEV(AGP) AS AGP_SD
FROM BASE
GROUP BY all
ORDER BY 1              
""")

agp_agg_sp = spark.sql(agp_agg_query)
# agp_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,Ecomm Aggregation
ecomm_txns_agg = (
f"""
WITH
EXPOSURE_BASE AS(
     SELECT * FROM {exposure_table}
   ),
-- Join TXNs to exposures
TXNS AS(
SELECT e.*,
     COALESCE(COUNT(DISTINCT t.TXN_ID),0) AS NUM_ORDERS,
     COALESCE(SUM(t.REVENUE),0) AS TOT_REVENUE,
     COALESCE(SUM(t.ITEMS),0) AS NUM_UNITS,
     COALESCE(SUM(t.SNAP_TENDER),0) AS TOT_SNAP,
     COALESCE(SUM(CASE WHEN BNC_SEGMENT IS NOT NULL THEN 1 ELSE 0 END)) AS ECOMM_BNC_COUNT,
     COALESCE(SUM(t.NET_SALES),0) AS TOT_NET_SALES
FROM EXPOSURE_BASE as e
LEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = 'ECOMM' AND (TXN_DTE >= '{EXP_START_DATE}' AND TXN_DTE <= '{EXP_END_DATE}') ) as t
   ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
   AND DATE(e.EXPOSURE_DATETIME) <= t.TXN_DTE  
LEFT JOIN (SELECT * FROM {bnc_table} WHERE TXN_DTE BETWEEN '{EXP_START_DATE}' AND '{EXP_END_DATE}') as b
   ON e.HOUSEHOLD_ID = b.HOUSEHOLD_ID
 GROUP BY all  
),
WINZ AS(
     SELECT
       APPROX_PERCENTILE(TOT_REVENUE,0.99) AS TOT_REVENUE_WIN99,
       APPROX_PERCENTILE(TOT_NET_SALES,0.99) AS TOT_NET_SALES_WIN99
     FROM TXNS
     WHERE HOUSEHOLD_ID IS NOT NULL
   ),
-- Join TXN info to Redemptions (primarily for TXN_DTE)
TXNS_REDEMPTIONS AS(
SELECT t.*
       , CLIENT_OFFER_ID
       , OFFER_TYPE_MOD
       , MKDN
FROM {combined_txn_table} as t
LEFT JOIN {redemptions_table} as r
   ON t.TXN_ID = r.TXN_ID
   AND (t.TXN_DTE >= '{EXP_START_DATE}' AND t.TXN_DTE <= '{EXP_END_DATE}')
WHERE TXN_LOCATION = 'ECOMM'
),
-- Filter Redemptions by Exposure Datetime
REDEMPTIONS_FILTERED AS(
SELECT e.*,
   -- REDEMPTIONS
   COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pd_redemptions
   , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'GR' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as gr_redemptions
   , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'MF' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as mf_redemptions
   , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SPD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as spd_redemptions
   , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PZN' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pzn_redemptions
   , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SC' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as sc_redemptions
   , COALESCE(COUNT(DISTINCT (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id)),0) as total_redemptions
   -- MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'PD' THEN MKDN ELSE 0 END),0) as pd_MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'GR' THEN MKDN ELSE 0 END),0) as gr_MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'MF' THEN MKDN ELSE 0 END),0) as mf_MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'SPD' THEN MKDN ELSE 0 END),0) as spd_MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'PZN' THEN MKDN ELSE 0 END),0) as pzn_MKDN
   , COALESCE(SUM(CASE WHEN offer_type_mod = 'SC' THEN MKDN ELSE 0 END),0) as sc_MKDN
   , COALESCE(SUM(MKDN),0) as total_mkdn
 FROM EXPOSURE_BASE as e
 LEFT JOIN TXNS_REDEMPTIONS as t
   ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
   AND DATE(e.EXPOSURE_DATETIME) <= t.TXN_DTE  
 GROUP BY all  
),
TXN_AGG AS(
SELECT VARIANT_ID,
       COUNT(DISTINCT {visitor_unit}) AS VISITORS
       , COUNT(DISTINCT IFF(NUM_ORDERS > 0, HOUSEHOLD_ID, NULL)) AS PURCHASING_CUSTOMERS
       , COUNT(DISTINCT IFF(TOT_REVENUE > (SELECT TOT_REVENUE_WIN99 FROM WINZ), HOUSEHOLD_ID, NULL)) AS WINSORIZED_CUSTOMERS
       , (SELECT TOT_REVENUE_WIN99 FROM WINZ) AS WINSORIZATION_THRESHOLD
 -- ORDERS
       , SUM(NUM_ORDERS) AS ECOMM_ORDERS_TOTAL
       , AVG(NUM_ORDERS) AS ECOMM_ORDERS_MEAN
       , STDDEV(NUM_ORDERS) AS ECOMM_ORDERS_SD
 -- UNITS
       , SUM(NUM_UNITS) AS ECOMM_UNITS_TOTAL
       , AVG(NUM_UNITS) AS ECOMM_UNITS_MEAN
       , STDDEV(NUM_UNITS) AS ECOMM_UNITS_SD
 --- REVENUE
       , SUM({metric_rpc_sql}) AS ECOMM_REVENUE_TOTAL
       , AVG({metric_rpc_sql}) AS ECOMM_REVENUE_MEAN
       , SUM({metric_rpc_sql}) / COUNT(DISTINCT HOUSEHOLD_ID) AS ECOMM_RPV
       , SUM(
             CASE WHEN TOT_REVENUE = 0
             THEN NULL
             ELSE {metric_rpc_sql} END) / COUNT(DISTINCT HOUSEHOLD_ID)
             AS ECOMM_NONZERO_RPV
       , STDDEV({metric_rpc_sql}) AS ECOMM_REVENUE_SD
       , AVG(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS ECOMM_REVENUE_NONZERO_MEAN
       , STDDEV(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS ECOMM_REVENUE_NONZERO_SD
 --- SNAP
       , SUM(TOT_SNAP) AS ECOMM_SNAP_TOTAL
       , AVG(TOT_SNAP) AS ECOMM_SNAP_MEAN
       , STDDEV(TOT_SNAP) AS ECOMM_SNAP_SD
 --- BNC COUNT
       , SUM(ECOMM_BNC_COUNT) as ECOMM_BNC_TOTAL
       , STDDEV(CASE WHEN ECOMM_BNC_COUNT = 0 THEN NULL ELSE ECOMM_BNC_COUNT END) AS ECOMM_BNC_TOTAL_SD
 --- NET SALES
       , SUM({metric_nspc_sql}) AS ECOMM_NET_SALES_TOTAL
       , AVG({metric_nspc_sql}) AS ECOMM_NET_SALES_MEAN
       , SUM({metric_nspc_sql}) / COUNT(DISTINCT HOUSEHOLD_ID) AS ECOMM_NET_SALES_RPV
       , SUM(
             CASE WHEN TOT_NET_SALES = 0
             THEN NULL
             ELSE {metric_nspc_sql} END) / COUNT(DISTINCT HOUSEHOLD_ID)
             AS ECOMM_NET_SALES_NONZERO_RPV
       , STDDEV({metric_nspc_sql}) AS ECOMM_NET_SALES_SD
       , AVG(CASE WHEN TOT_NET_SALES = 0 THEN NULL ELSE {metric_nspc_sql} END) AS ECOMM_NET_SALES_NONZERO_MEAN
       , STDDEV(CASE WHEN TOT_NET_SALES = 0 THEN NULL ELSE {metric_nspc_sql} END) AS ECOMM_NET_SALES_NONZERO_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS ECOMM_AOV
        , COVAR_SAMP({metric_rpc_sql},NUM_ORDERS) AS ECOMM_COV_REVENUE_ORDERS
        , AVG(IFF(NUM_UNITS > 0, NUM_UNITS, NULL)) / AVG(IFF(NUM_ORDERS > 0 , NUM_ORDERS, NULL)) AS ECOMM_UPO
        , COVAR_SAMP(NUM_UNITS,NUM_ORDERS) AS ECOMM_COV_UNITS_ORDERS
FROM TXNS
GROUP BY all
),
REDEMPTIONS_AGG AS (
SELECT VARIANT_ID,
 --- UNIQUE_REDEEMING_HH
       COUNT(DISTINCT IFF(TOTAL_REDEMPTIONS > 0, HOUSEHOLD_ID, NULL)) AS REDEEMING_COUNT_ECOMM
       , AVG(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_ECOMM_MEAN
       , STD(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_EECOMM_SD
 --- REDEMPTIONS TOTAL
     , SUM(TOTAL_REDEMPTIONS) as ECOMM_REDEMPTIONS_TOTAL
     , SUM(TOTAL_REDEMPTIONS) / COUNT(DISTINCT HOUSEHOLD_ID) as ECOMM_REDEMPTIONS_AVG_REAL
     , AVG(TOTAL_REDEMPTIONS) as ECOMM_REDEMPTIONS_MEAN
     , STDDEV(TOTAL_REDEMPTIONS) as ECOMM_REDEMPTIONS_SD
 --- MARKDOWN TOTAL
     , SUM(TOTAL_MKDN) as ECOMM_MKDN_TOTAL
     , SUM(TOTAL_MKDN) / COUNT(DISTINCT HOUSEHOLD_ID) as ECOMM_MKDN_AVG_REAL
     , AVG(TOTAL_MKDN) as ECOMM_MKDN_MEAN
     , STDDEV(TOTAL_MKDN) as ECOMM_MKDN_SD
 --- REDEMPTIONS BREAKDOWN
     , SUM(pd_redemptions) AS pd_redemptions_TOTAL
     , SUM(gr_redemptions) AS gr_redemptions_TOTAL
     , SUM(mf_redemptions) AS mf_redemptions_TOTAL
     , SUM(spd_redemptions) AS spd_redemptions_TOTAL
     , SUM(pzn_redemptions) AS pzn_redemptions_TOTAL
     , SUM(sc_redemptions) AS sc_redemptions_TOTAL
     , AVG(pd_redemptions) AS pd_redemptions_MEAN
     , AVG(gr_redemptions) AS gr_redemptions_MEAN
     , AVG(mf_redemptions) AS mf_redemptions_MEAN
     , AVG(spd_redemptions) AS spd_redemptions_MEAN
     , AVG(pzn_redemptions) AS pzn_redemptions_MEAN
     , AVG(sc_redemptions) AS sc_redemptions_MEAN
     , STDDEV(pd_redemptions) AS pd_redemptions_SD
     , STDDEV(gr_redemptions) AS gr_redemptions_SD
     , STDDEV(mf_redemptions) AS mf_redemptions_SD
     , STDDEV(spd_redemptions) AS spd_redemptions_SD
     , STDDEV(pzn_redemptions) AS pzn_redemptions_SD
     , STDDEV(sc_redemptions) AS sc_redemptions_SD
 --- MARKDOWN BREAKDOWN
     , SUM(pd_MKDN) AS pd_MKDN_TOTAL
     , SUM(gr_MKDN) AS gr_MKDN_TOTAL
     , SUM(mf_MKDN) AS mf_MKDN_TOTAL
     , SUM(spd_MKDN) AS spd_MKDN_TOTAL
     , SUM(pzn_MKDN) AS pzn_MKDN_TOTAL
     , SUM(sc_MKDN) AS sc_MKDN_TOTAL
     , AVG(pd_MKDN) AS pd_MKDN_MEAN
     , AVG(gr_MKDN) AS gr_MKDN_MEAN
     , AVG(mf_MKDN) AS mf_MKDN_MEAN
     , AVG(spd_MKDN) AS spd_MKDN_MEAN
     , AVG(pzn_MKDN) AS pzn_MKDN_MEAN
     , AVG(sc_MKDN) AS sc_MKDN_MEAN
     , STDDEV(pd_MKDN) AS pd_MKDN_SD
     , STDDEV(gr_MKDN) AS gr_MKDN_SD
     , STDDEV(mf_MKDN) AS mf_MKDN_SD
     , STDDEV(spd_MKDN) AS spd_MKDN_SD
     , STDDEV(pzn_MKDN) AS pzn_MKDN_SD
     , STDDEV(sc_MKDN) AS sc_MKDN_SD
FROM REDEMPTIONS_FILTERED
GROUP BY all
)
SELECT t.*, r.* EXCEPT(r.VARIANT_ID)
     FROM TXN_AGG as t
     JOIN REDEMPTIONS_AGG as r
     ON t.VARIANT_ID = r.VARIANT_ID
"""
)
ecomm_agg_sp = spark.sql(ecomm_txns_agg)
# ecomm_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,In-Store Aggregation
store_txns_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

-- Join TXNs to exposures
TXNS AS(
SELECT e.*,
      COALESCE(COUNT(DISTINCT t.TXN_ID),0) AS NUM_ORDERS
      , COALESCE(SUM(t.REVENUE),0) AS TOT_REVENUE
      , COALESCE(SUM(t.ITEMS),0) AS NUM_UNITS
      , COALESCE(SUM(t.SNAP_TENDER),0) AS TOT_SNAP
FROM EXPOSURE_BASE as e
LEFT JOIN (SELECT * FROM {combined_txn_table} WHERE TXN_LOCATION = 'STORE' AND (TXN_DTE >= '{EXP_START_DATE}' AND TXN_DTE <= '{EXP_END_DATE}') ) as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
    AND DATE(e.EXPOSURE_DATETIME) <= t.TXN_DTE  
  GROUP BY all  
),

WINZ AS(
      SELECT
        APPROX_PERCENTILE(TOT_REVENUE,0.99) AS TOT_REVENUE_WIN99
      FROM TXNS
      WHERE HOUSEHOLD_ID IS NOT NULL
    ),

-- Join TXN info to Redemptions (primarily for TXN_DTE)
TXNS_REDEMPTIONS AS(
SELECT t.*
        , CLIENT_OFFER_ID
        , OFFER_TYPE_MOD
        , MKDN
FROM {combined_txn_table} as t
LEFT JOIN {redemptions_table} as r
    ON t.TXN_ID = r.TXN_ID
    AND (t.TXN_DTE >= '{EXP_START_DATE}' AND t.TXN_DTE <= '{EXP_END_DATE}')
WHERE TXN_LOCATION = 'STORE'
),

-- Filter Redemptions by Exposure Datetime
REDEMPTIONS_FILTERED AS(
SELECT e.*,
    -- REDEMPTIONS
    COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pd_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'GR' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as gr_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'MF' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as mf_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SPD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as spd_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PZN' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pzn_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SC' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as sc_redemptions
    , COALESCE(COUNT(DISTINCT (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id)),0) as total_redemptions
    -- MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'PD' THEN MKDN ELSE 0 END),0) as pd_MKDN 
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'GR' THEN MKDN ELSE 0 END),0) as gr_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'MF' THEN MKDN ELSE 0 END),0) as mf_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'SPD' THEN MKDN ELSE 0 END),0) as spd_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'PZN' THEN MKDN ELSE 0 END),0) as pzn_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'SC' THEN MKDN ELSE 0 END),0) as sc_MKDN
    , COALESCE(SUM(MKDN),0) as total_mkdn
  FROM EXPOSURE_BASE as e
  LEFT JOIN TXNS_REDEMPTIONS as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
    AND DATE(e.EXPOSURE_DATETIME) <= t.TXN_DTE  
  GROUP BY all 
),

TXN_AGG AS(
SELECT VARIANT_ID,
        COUNT(DISTINCT {visitor_unit}) AS VISITORS
        , COUNT(DISTINCT IFF(NUM_ORDERS > 0, HOUSEHOLD_ID, NULL)) AS PURCHASING_CUSTOMERS
        , COUNT(DISTINCT IFF(TOT_REVENUE > (SELECT TOT_REVENUE_WIN99 FROM WINZ), HOUSEHOLD_ID, NULL)) AS WINSORIZED_CUSTOMERS
        , (SELECT TOT_REVENUE_WIN99 FROM WINZ) AS WINSORIZATION_THRESHOLD
  -- ORDERS
        , SUM(NUM_ORDERS) AS STORE_ORDERS_TOTAL
        , AVG(NUM_ORDERS) AS STORE_ORDERS_MEAN
        , STDDEV(NUM_ORDERS) AS STORE_ORDERS_SD
  -- UNITS
        , SUM(NUM_UNITS) AS STORE_UNITS_TOTAL
        , AVG(NUM_UNITS) AS STORE_UNITS_MEAN
        , STDDEV(NUM_UNITS) AS STORE_UNITS_SD
  --- REVENUE
        , SUM({metric_rpc_sql}) AS STORE_REVENUE_TOTAL
        , AVG({metric_rpc_sql}) AS STORE_REVENUE_MEAN
        , SUM({metric_rpc_sql}) / COUNT(DISTINCT HOUSEHOLD_ID) AS STORE_RPV
        , SUM(
              CASE WHEN TOT_REVENUE = 0 
              THEN NULL ELSE {metric_rpc_sql} END) / COUNT(DISTINCT HOUSEHOLD_ID) AS STORE_NONZERO_RPV
        , STDDEV({metric_rpc_sql}) AS STORE_REVENUE_SD
  --- NON ZERO REVENUE
        ,AVG(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS STORE_REVENUE_NONZERO_MEAN
        ,STDDEV(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS STORE_REVENUE_NONZERO_SD
  
  --- SNAP
        , SUM(TOT_SNAP) AS STORE_SNAP_TOTAL
        , AVG(TOT_SNAP) AS STORE_SNAP_MEAN
        , STDDEV(TOT_SNAP) AS STORE_SNAP_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS STORE_AOV
        , COVAR_SAMP({metric_rpc_sql},NUM_ORDERS) AS STORE_COV_REVENUE_ORDERS
        , AVG(IFF(NUM_UNITS > 0, NUM_UNITS, NULL)) / AVG(IFF(NUM_ORDERS > 0 , NUM_ORDERS, NULL)) AS STORE_UPO
        , COVAR_SAMP(NUM_UNITS,NUM_ORDERS) AS STORE_COV_UNITS_ORDERS
FROM TXNS 
GROUP BY all
),

REDEMPTIONS_AGG AS (
SELECT VARIANT_ID,
  --- UNIQUE_REDEEMING_HH
        COUNT(DISTINCT IFF(TOTAL_REDEMPTIONS > 0, HOUSEHOLD_ID, NULL)) AS REDEEMING_COUNT_STORE
        , AVG(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_STORE_MEAN
        , STD(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_ESTORE_SD
  --- REDEMPTIONS TOTAL
      , SUM(TOTAL_REDEMPTIONS) as STORE_REDEMPTIONS_TOTAL
      , SUM(TOTAL_REDEMPTIONS) / COUNT(DISTINCT HOUSEHOLD_ID) as STORE_REDEMPTIONS_AVG_REAL
      , AVG(TOTAL_REDEMPTIONS) as STORE_REDEMPTIONS_MEAN
      , STDDEV(TOTAL_REDEMPTIONS) as STORE_REDEMPTIONS_SD
  --- MARKDOWN TOTAL
      , SUM(TOTAL_MKDN) as STORE_MKDN_TOTAL
      , SUM(TOTAL_MKDN) / COUNT(DISTINCT HOUSEHOLD_ID) as STORE_MKDN_AVG_REAL
      , AVG(TOTAL_MKDN) as STORE_MKDN_MEAN
      , STDDEV(TOTAL_MKDN) as STORE_MKDN_SD
  --- REDEMPTIONS BREAKDOWN
      , SUM(pd_redemptions) AS pd_redemptions_TOTAL
      , SUM(gr_redemptions) AS gr_redemptions_TOTAL
      , SUM(mf_redemptions) AS mf_redemptions_TOTAL
      , SUM(spd_redemptions) AS spd_redemptions_TOTAL
      , SUM(pzn_redemptions) AS pzn_redemptions_TOTAL
      , SUM(sc_redemptions) AS sc_redemptions_TOTAL
      , AVG(pd_redemptions) AS pd_redemptions_MEAN
      , AVG(gr_redemptions) AS gr_redemptions_MEAN
      , AVG(mf_redemptions) AS mf_redemptions_MEAN
      , AVG(spd_redemptions) AS spd_redemptions_MEAN
      , AVG(pzn_redemptions) AS pzn_redemptions_MEAN
      , AVG(sc_redemptions) AS sc_redemptions_MEAN
      , STDDEV(pd_redemptions) AS pd_redemptions_SD
      , STDDEV(gr_redemptions) AS gr_redemptions_SD
      , STDDEV(mf_redemptions) AS mf_redemptions_SD
      , STDDEV(spd_redemptions) AS spd_redemptions_SD
      , STDDEV(pzn_redemptions) AS pzn_redemptions_SD
      , STDDEV(sc_redemptions) AS sc_redemptions_SD
  --- MARKDOWN BREAKDOWN
      , SUM(pd_MKDN) AS pd_MKDN_TOTAL
      , SUM(gr_MKDN) AS gr_MKDN_TOTAL
      , SUM(mf_MKDN) AS mf_MKDN_TOTAL
      , SUM(spd_MKDN) AS spd_MKDN_TOTAL
      , SUM(pzn_MKDN) AS pzn_MKDN_TOTAL
      , SUM(sc_MKDN) AS sc_MKDN_TOTAL
      , AVG(pd_MKDN) AS pd_MKDN_MEAN
      , AVG(gr_MKDN) AS gr_MKDN_MEAN
      , AVG(mf_MKDN) AS mf_MKDN_MEAN
      , AVG(spd_MKDN) AS spd_MKDN_MEAN
      , AVG(pzn_MKDN) AS pzn_MKDN_MEAN
      , AVG(sc_MKDN) AS sc_MKDN_MEAN
      , STDDEV(pd_MKDN) AS pd_MKDN_SD
      , STDDEV(gr_MKDN) AS gr_MKDN_SD
      , STDDEV(mf_MKDN) AS mf_MKDN_SD
      , STDDEV(spd_MKDN) AS spd_MKDN_SD
      , STDDEV(pzn_MKDN) AS pzn_MKDN_SD
      , STDDEV(sc_MKDN) AS sc_MKDN_SD
FROM REDEMPTIONS_FILTERED 
GROUP BY all
)

SELECT t.*, r.* EXCEPT(r.VARIANT_ID)
      FROM TXN_AGG as t
      JOIN REDEMPTIONS_AGG as r
      ON t.VARIANT_ID = r.VARIANT_ID
""")

store_agg_sp = spark.sql(store_txns_agg)
# store_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,TXNS Aggregation
txns_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

TXNS AS(
SELECT e.*
      , COALESCE(COUNT(DISTINCT t.TXN_ID),0) AS NUM_ORDERS
      , COALESCE(SUM(t.REVENUE),0) AS TOT_REVENUE
      , COALESCE(SUM(t.ITEMS),0) AS NUM_UNITS
      , COALESCE(SUM(t.SNAP_TENDER),0) AS TOT_SNAP
FROM EXPOSURE_BASE as e
LEFT JOIN {combined_txn_table} as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
    AND DATE(e.EXPOSURE_DATETIME) <= t.TXN_DTE 
    AND (t.TXN_DTE >= '{EXP_START_DATE}' AND t.TXN_DTE <= '{EXP_END_DATE}') 
  GROUP BY all 
),

WINZ AS(
      SELECT
        APPROX_PERCENTILE(TOT_REVENUE,0.99) AS TOT_REVENUE_WIN99
      FROM TXNS
      WHERE HOUSEHOLD_ID IS NOT NULL
)

SELECT VARIANT_ID
        , COUNT(DISTINCT {visitor_unit}) AS VISITORS
        , COUNT(DISTINCT IFF(NUM_ORDERS > 0, HOUSEHOLD_ID, NULL)) AS PURCHASING_CUSTOMERS
        , COUNT(DISTINCT IFF(TOT_REVENUE > (SELECT TOT_REVENUE_WIN99 FROM WINZ), HOUSEHOLD_ID, NULL)) AS WINSORIZED_CUSTOMERS
        , (SELECT TOT_REVENUE_WIN99 FROM WINZ) AS WINSORIZATION_THRESHOLD
  -- ORDERS
        , SUM(NUM_ORDERS) AS COMBINED_ORDERS_TOTAL
        , AVG(NUM_ORDERS) AS COMBINED_ORDERS_MEAN
        , STDDEV(NUM_ORDERS) AS COMBINED_ORDERS_SD
  -- UNITS
        , SUM(NUM_UNITS) AS COMBINED_UNITS_TOTAL
        , AVG(NUM_UNITS) AS COMBINED_UNITS_MEAN
        , STDDEV(NUM_UNITS) AS COMBINED_UNITS_SD
  --- REVENUE
      , SUM({metric_rpc_sql}) AS COMBINED_REVENUE_TOTAL
      , AVG({metric_rpc_sql}) AS COMBINED_REVENUE_MEAN
      , SUM({metric_rpc_sql}) / COUNT(DISTINCT HOUSEHOLD_ID) AS COMBINED_RPV
      , SUM(CASE WHEN TOT_REVENUE > 0 THEN NULL ELSE {metric_rpc_sql} END) / COUNT(DISTINCT HOUSEHOLD_ID) AS COMBINED_NONZERO_RPV
      , STDDEV({metric_rpc_sql}) AS COMBINED_REVENUE_SD
  --- NON ZERO REVENUE
      ,AVG(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS COMBINED_REVENUE_NONZERO_MEAN
      ,STDDEV(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE {metric_rpc_sql} END) AS COMBINED_REVENUE_NONZERO_SD
  --- SNAP
        , SUM(TOT_SNAP) AS COMBINED_SNAP_TOTAL
        , AVG(TOT_SNAP) AS COMBINED_SNAP_MEAN
        , STDDEV(TOT_SNAP) AS COMBINED_SNAP_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, {metric_rpc_sql}, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS COMBINED_AOV
        , COVAR_SAMP({metric_rpc_sql},NUM_ORDERS) AS COMBINED_COV_REVENUE_ORDERS
        , AVG(IFF(NUM_UNITS > 0, NUM_UNITS, NULL)) / AVG(IFF(NUM_ORDERS > 0 , NUM_ORDERS, NULL)) AS COMBINED_UPO
        , COVAR_SAMP(NUM_UNITS,NUM_ORDERS) AS COMBINED_COV_UNITS_ORDERS
FROM TXNS 
GROUP BY all
"""
)

txns_sp = spark.sql(txns_agg)
#txns_sp.display()

# COMMAND ----------

# DBTITLE 1,Redemptions Aggregation
redemptions_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

-- Join TXN info to Redemptions (primarily for TXN_DTE)
TXNS_REDEMPTIONS AS(
SELECT t.*
        , CLIENT_OFFER_ID
        , OFFER_TYPE_MOD
        , MKDN
FROM {combined_txn_table} as t
LEFT JOIN {redemptions_table} as r
    ON t.TXN_ID = r.TXN_ID
    AND (t.TXN_DTE >= '{EXP_START_DATE}' AND t.TXN_DTE <= '{EXP_END_DATE}')
),

-- Filter Redemptions by Exposure Datetime
REDEMPTIONS_FILTERED AS(
SELECT e.*,
    -- REDEMPTIONS
    COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pd_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'GR' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as gr_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'MF' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as mf_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SPD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as spd_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PZN' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as pzn_redemptions
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SC' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id) ELSE NULL END),0) as sc_redemptions
    , COALESCE(COUNT(DISTINCT (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.txn_id)),0) as total_redemptions
    -- MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'PD' THEN MKDN ELSE 0 END),0) as pd_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'GR' THEN MKDN ELSE 0 END),0) as gr_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'MF' THEN MKDN ELSE 0 END),0) as mf_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'SPD' THEN MKDN ELSE 0 END),0) as spd_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'PZN' THEN MKDN ELSE 0 END),0) as pzn_MKDN
    , COALESCE(SUM(CASE WHEN offer_type_mod = 'SC' THEN MKDN ELSE 0 END),0) as sc_MKDN
    , COALESCE(SUM(MKDN),0) as total_mkdn
  FROM EXPOSURE_BASE as e
  LEFT JOIN TXNS_REDEMPTIONS as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
    AND t.TXN_DTE BETWEEN DATE(e.EXPOSURE_DATETIME) AND '{EXP_END_DATE}'  
  GROUP BY all  
)

SELECT VARIANT_ID
      , COUNT(DISTINCT {visitor_unit}) AS VISITORS
  --- UNIQUE_REDEEMING_HH
        , COUNT(DISTINCT IFF(TOTAL_REDEMPTIONS > 0, HOUSEHOLD_ID, NULL)) AS REDEEMING_COUNT_COMBINED
        , AVG(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_COMBINED_MEAN
        , STD(IFF(TOTAL_REDEMPTIONS > 0, TOTAL_REDEMPTIONS, NULL)) AS RPO_ECOMBINED_SD
  --- REDEMPTIONS TOTAL
      , SUM(TOTAL_REDEMPTIONS) as REDEMPTIONS_TOTAL
      , SUM(TOTAL_REDEMPTIONS) / COUNT(DISTINCT HOUSEHOLD_ID) as REDEMPTIONS_AVG_REAL
      , AVG(TOTAL_REDEMPTIONS) as REDEMPTIONS_MEAN
      , STDDEV(TOTAL_REDEMPTIONS) as REDEMPTIONS_SD
  --- MARKDOWN TOTAL
      , SUM(TOTAL_MKDN) as MKDN_TOTAL
      , SUM(TOTAL_MKDN) / COUNT(DISTINCT HOUSEHOLD_ID) as MKDN_AVG_REAL
      , AVG(TOTAL_MKDN) as MKDN_MEAN
      , STDDEV(TOTAL_MKDN) as MKDN_SD
  --- REDEMPTIONS BREAKDOWN
      , SUM(pd_redemptions) AS pd_redemptions_TOTAL
      , SUM(gr_redemptions) AS gr_redemptions_TOTAL
      , SUM(mf_redemptions) AS mf_redemptions_TOTAL
      , SUM(spd_redemptions) AS spd_redemptions_TOTAL
      , SUM(pzn_redemptions) AS pzn_redemptions_TOTAL
      , SUM(sc_redemptions) AS sc_redemptions_TOTAL
      , AVG(pd_redemptions) AS pd_redemptions_MEAN
      , AVG(gr_redemptions) AS gr_redemptions_MEAN
      , AVG(mf_redemptions) AS mf_redemptions_MEAN
      , AVG(spd_redemptions) AS spd_redemptions_MEAN
      , AVG(pzn_redemptions) AS pzn_redemptions_MEAN
      , AVG(sc_redemptions) AS sc_redemptions_MEAN
      , STDDEV(pd_redemptions) AS pd_redemptions_SD
      , STDDEV(gr_redemptions) AS gr_redemptions_SD
      , STDDEV(mf_redemptions) AS mf_redemptions_SD
      , STDDEV(spd_redemptions) AS spd_redemptions_SD
      , STDDEV(pzn_redemptions) AS pzn_redemptions_SD
      , STDDEV(sc_redemptions) AS sc_redemptions_SD
  --- MARKDOWN BREAKDOWN
      , SUM(pd_MKDN) AS pd_MKDN_TOTAL
      , SUM(gr_MKDN) AS gr_MKDN_TOTAL
      , SUM(mf_MKDN) AS mf_MKDN_TOTAL
      , SUM(spd_MKDN) AS spd_MKDN_TOTAL
      , SUM(pzn_MKDN) AS pzn_MKDN_TOTAL
      , SUM(sc_MKDN) AS sc_MKDN_TOTAL
      , AVG(pd_MKDN) AS pd_MKDN_MEAN
      , AVG(gr_MKDN) AS gr_MKDN_MEAN
      , AVG(mf_MKDN) AS mf_MKDN_MEAN
      , AVG(spd_MKDN) AS spd_MKDN_MEAN
      , AVG(pzn_MKDN) AS pzn_MKDN_MEAN
      , AVG(sc_MKDN) AS sc_MKDN_MEAN
      , STDDEV(pd_MKDN) AS pd_MKDN_SD
      , STDDEV(gr_MKDN) AS gr_MKDN_SD
      , STDDEV(mf_MKDN) AS mf_MKDN_SD
      , STDDEV(spd_MKDN) AS spd_MKDN_SD
      , STDDEV(pzn_MKDN) AS pzn_MKDN_SD
      , STDDEV(sc_MKDN) AS sc_MKDN_SD
FROM REDEMPTIONS_FILTERED 
GROUP BY all
"""
)

redemptions_sp = spark.sql(redemptions_agg)
#redemptions_sp.display()

# COMMAND ----------

# DBTITLE 1,Bonus Points Query
bp_query = f"""
select SAFE_CAST(t.HOUSEHOLD_ID as BIGINT) as HOUSEHOLD_ID
      , DATE(t.TRANSACTION_TS) as TXN_DTE
      , SUM(p.POINTS_EARNED_NBR) as BONUS_POINTS_EARNED
from gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.EPE_TRANSACTION_HEADER  as t 
JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_loyl.EPE_TRANSACTION_HEADER_SAVING_POINTS  as p 
      ON t.TRANSACTION_INTEGRATION_ID = p.TRANSACTION_INTEGRATION_ID
where 1=1
      and OFFER_ID not in (44646442,64035934,90120515)  --- Base points offer ID that should be excluded to get to Bonus points only
      and DATE(TRANSACTION_TS) BETWEEN '{EXP_START_DATE}' AND '{EXP_END_DATE}'
      and p.dw_current_version_ind = True
      and p.dw_logical_delete_ind = False
GROUP BY all
"""
# print(bp_query)
bp_sp = bc.read_gcp_table(bp_query)
bp_sp.cache()
bp_sp.createOrReplaceTempView("bonus_points_temp")


# COMMAND ----------

# DBTITLE 1,Bonus Points Aggregation
bp_agg_query = f"""
WITH
BONUS_POINTS_AGG AS(
SELECT e.*
      , COALESCE(SUM(BONUS_POINTS_EARNED),0) as BONUS_POINTS
      , .015*COALESCE(SUM(BONUS_POINTS_EARNED),0) as ESTIMATED_MKDN
FROM {exposure_table} as e
LEFT JOIN bonus_points_temp as p
      on e.HOUSEHOLD_ID = p.household_id
      and e.exposure_datetime <= p.TXN_DTE
group by all
)

SELECT VARIANT_ID
      , COUNT(DISTINCT CASE WHEN BONUS_POINTS > 0 THEN HOUSEHOLD_ID ELSE NULL END) as BP_EARNING_HOUSEHOLDS
      , SUM(BONUS_POINTS) as BONUS_POINTS_TOTAL
      , AVG(BONUS_POINTS) as BONUS_POINTS_MEAN
      , STDDEV(BONUS_POINTS) as BONUS_POINTS_SD
      , SUM(ESTIMATED_MKDN) as ESTIMATED_MKDN_TOTAL
      , AVG(ESTIMATED_MKDN) as ESTIMATED_MKDN_MEAN
      , STDDEV(ESTIMATED_MKDN) as ESTIMATED_MKDN_SD
FROM BONUS_POINTS_AGG
group by all
"""
bp_agg_sp = spark.sql(bp_agg_query)
redemptions_sp = redemptions_sp.join(bp_agg_sp, on='VARIANT_ID', how='left')

# COMMAND ----------

# DBTITLE 1,Clips Aggregation
clips_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

-- Filter Clips by Exposure Datetime
EXPOSURE_CLIPS AS(
SELECT e.*,
    -- CLIPS
    COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SC' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as sc_clips
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'GR' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as gr_clips
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'MF' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as mf_clips
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as pd_clips
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'SPD' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as spd_clips
    , COALESCE(COUNT(DISTINCT CASE WHEN offer_type_mod = 'PZN' THEN (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts) ELSE NULL END),0) as pzn_clips
    , COALESCE(COUNT(DISTINCT (e.HOUSEHOLD_ID || CLIENT_OFFER_ID || t.clip_ts)),0) as total_clips
  FROM EXPOSURE_BASE as e
  LEFT JOIN {clips_table} as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID 
    AND (e.EXPOSURE_DATETIME <= t.clip_ts) AND  (t.clip_ts <= '{EXP_END_DATE}')
  GROUP BY all 
)

SELECT VARIANT_ID,
      COUNT(DISTINCT {visitor_unit}) AS VISITORS
  --- UNIQUE_CLIPPING_HH
        , COUNT(DISTINCT IFF(total_clips > 0, HOUSEHOLD_ID, NULL)) AS UNIQUE_CLIPPING_HH
        , AVG(IFF(total_clips > 0, total_clips, NULL)) AS CLIPS_PER_HH_NON_ZERO_MEAN
        , STDDEV(IFF(total_clips > 0, total_clips, NULL)) AS CLIPS_PER_HH_NON_ZERO_SD
  --- CLIPS TOTAL
      , SUM(total_clips) as CLIPS_TOTAL
      , SUM(total_clips) / COUNT(DISTINCT HOUSEHOLD_ID) as CLIPS_AVG_REAL
      , AVG(total_clips) as CLIPS_MEAN
      , STDDEV(total_clips) as CLIPS_SD
  --- CLIPS BREAKDOWN
      , SUM(pd_clips) AS pd_clips_TOTAL
      , SUM(gr_clips) AS gr_clips_TOTAL
      , SUM(mf_clips) AS mf_clips_TOTAL
      , SUM(spd_clips) AS spd_clips_TOTAL
      , SUM(pzn_clips) AS pzn_clips_TOTAL
      , SUM(sc_clips) AS sc_clips_TOTAL
      , AVG(pd_clips) AS pd_clips_MEAN
      , AVG(gr_clips) AS gr_clips_MEAN
      , AVG(mf_clips) AS mf_clips_MEAN
      , AVG(spd_clips) AS spd_clips_MEAN
      , AVG(pzn_clips) AS pzn_clips_MEAN
      , AVG(sc_clips) AS sc_clips_MEAN
      , STDDEV(pd_clips) AS pd_clips_SD
      , STDDEV(gr_clips) AS gr_clips_SD
      , STDDEV(mf_clips) AS mf_clips_SD
      , STDDEV(spd_clips) AS spd_clips_SD
      , STDDEV(pzn_clips) AS pzn_clips_SD
      , STDDEV(sc_clips) AS sc_clips_SD
FROM EXPOSURE_CLIPS 
GROUP BY all
"""
)

clips_agg_sp = spark.sql(clips_agg)
#clips_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,Basket Health Aggregation
basket_health_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

-- Filter Basket Health by Exposure Datetime
  BASKET_HEALTH_EXPOSED AS(
  SELECT e.*, 
    COALESCE(COUNT(DISTINCT CASE WHEN bh.OVERALL_CATEGORY = 'A - PERFECT' THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS Perfect_rate,
    COALESCE(COUNT(DISTINCT CASE WHEN bh.OVERALL_CATEGORY = 'B - GREAT' THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS great_rate,
    COALESCE(COUNT(DISTINCT CASE WHEN bh.OVERALL_CATEGORY = 'C - ACCEPTABLE' THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS acceptable_rate,
    COALESCE(COUNT(DISTINCT CASE WHEN bh.OVERALL_CATEGORY = 'D - NEEDS_IMPROVEMENT' THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS needs_improvement_rate,
    COALESCE(COUNT(DISTINCT CASE WHEN bh.OVERALL_CATEGORY = 'E - POOR' THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS poor_rate,
    -- Net_Basket_rate
    COALESCE(COUNT(DISTINCT CASE WHEN (bh.OVERALL_CATEGORY = 'A - PERFECT' OR bh.OVERALL_CATEGORY = 'B - GREAT') THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) -
    COALESCE(COUNT(DISTINCT CASE WHEN (bh.OVERALL_CATEGORY = 'D - NEEDS_IMPROVEMENT' OR bh.OVERALL_CATEGORY = 'E - POOR') THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS Net_Basket_rate,
    -- AB_RATE
    COALESCE(COUNT(DISTINCT CASE WHEN (bh.OVERALL_CATEGORY = 'A - PERFECT' OR bh.OVERALL_CATEGORY = 'B - GREAT') THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS AB_RATE,
    -- DE_RATE
    COALESCE(COUNT(DISTINCT CASE WHEN (bh.OVERALL_CATEGORY = 'D - NEEDS_IMPROVEMENT' OR bh.OVERALL_CATEGORY = 'E - POOR') THEN TXN_ID ELSE NULL END)/COUNT(DISTINCT TXN_ID), 0) AS DE_RATE,
    COUNT(DISTINCT TXN_ID) AS TOTAL_TXNS
  FROM EXPOSURE_BASE AS e
  LEFT JOIN {basket_health_table} as bh
    ON e.HOUSEHOLD_ID = bh.HOUSEHOLD_ID
    AND bh.TXN_DTE BETWEEN DATE(e.EXPOSURE_DATETIME) AND '{EXP_END_DATE}'
  GROUP BY ALL
)

SELECT VARIANT_ID, 
      COUNT(DISTINCT {visitor_unit}) AS VISITORS
    , COUNT(DISTINCT HOUSEHOLD_ID) AS UNIQUE_BASKET_HEALTH_HH
    , SUM(NET_BASKET_RATE) as BASKET_RATE_TOTAL
    , AVG(NET_BASKET_RATE) as BASKET_RATE_MEAN
    , STDDEV(NET_BASKET_RATE) as BASKET_RATE_SD
    , SUM(AB_RATE) as AB_RATE_TOTAL
    , AVG(AB_RATE) as AB_RATE_MEAN
    , STDDEV(AB_RATE) as AB_RATE_SD
    , SUM(DE_RATE) as DE_RATE_TOTAL
    , AVG(DE_RATE) as DE_RATE_MEAN
    , STDDEV(DE_RATE) as DE_RATE_SD
    , SUM(TOTAL_TXNS) as TOTAL_BASKET_HEALTH_TXNS
FROM BASKET_HEALTH_EXPOSED
GROUP BY ALL
"""
)

basket_health_agg_sp = spark.sql(basket_health_agg)
#clips_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,Gas Aggregation
gas_txns_agg = (
f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
),

GAS_BASE AS(
SELECT e.*
  , COALESCE(COUNT(DISTINCT s.TXN_ID),0) AS NUM_ORDERS
  , COALESCE(SUM(s.REVENUE),0) AS TOT_REVENUE
  , COALESCE(SUM(s.GAS_REWARD_REDEMPTIONS),0) as GAS_REWARD_REDEMPTIONS
  , -1*COALESCE(SUM(s.GAS_MKDN),0) as GAS_MKDN
  FROM EXPOSURE_BASE as e
  LEFT JOIN {gas_table} as s
    ON e.HOUSEHOLD_ID = s.HOUSEHOLD_ID
    AND e.EXPOSURE_DATETIME <= s.TXN_DTE 
    AND (s.TXN_DTE >= '{EXP_START_DATE}' AND s.TXN_DTE <= '{EXP_END_DATE}') 
  GROUP BY all
)

SELECT VARIANT_ID AS VARIANT_ID_GAS
        , COUNT(DISTINCT {visitor_unit}) AS VISITORS
        , COUNT(DISTINCT IFF(NUM_ORDERS > 0, HOUSEHOLD_ID, NULL)) AS GAS_VISITORS
  -- ORDERS
        , SUM(NUM_ORDERS) AS GAS_ORDERS_TOTAL
        , AVG(NUM_ORDERS) AS GAS_ORDERS_MEAN
        , STDDEV(NUM_ORDERS) AS GAS_ORDERS_SD
  --- GAS REVENUE
        , SUM(TOT_REVENUE) AS GAS_REVENUE_TOTAL
        , AVG(TOT_REVENUE) AS GAS_REVENUE_MEAN
        , STDDEV(TOT_REVENUE) AS GAS_REVENUE_SD
   --- NON ZERO REVENUE
        ,AVG(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE TOT_REVENUE END) AS GAS_REVENUE_NONZERO_MEAN
        ,STDDEV(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE TOT_REVENUE END) AS GAS_REVENUE_NONZERO_SD
  --- GAS_REWARD_REDEMPTIONS
        , SUM(GAS_REWARD_REDEMPTIONS) AS GAS_REWARD_REDEMPTIONS_TOTAL
        , AVG(GAS_REWARD_REDEMPTIONS) AS GAS_REWARD_REDEMPTIONS_MEAN
        , STDDEV(GAS_REWARD_REDEMPTIONS) AS GAS_REWARD_REDEMPTIONS_SD
  --- GAS_MKDN
        , SUM(GAS_MKDN) AS GAS_MKDN_TOTAL
        , AVG(GAS_MKDN) AS GAS_MKDN_MEAN
        , STDDEV(GAS_MKDN) AS GAS_MKDN_SD
  --- Ratio Metrics
        , AVG(IFF(TOT_REVENUE > 0, TOT_REVENUE, NULL)) / AVG(IFF(NUM_ORDERS > 0, NUM_ORDERS, NULL)) AS GAS_AOV
        , COVAR_SAMP(TOT_REVENUE,NUM_ORDERS) AS GAS_COV_REVENUE_ORDERS
        , SUM(TOT_REVENUE) / COUNT(DISTINCT HOUSEHOLD_ID) AS GAS_RPV
        , SUM(CASE WHEN TOT_REVENUE = 0 THEN NULL ELSE TOT_REVENUE END) / COUNT(DISTINCT HOUSEHOLD_ID) AS GAS_NONZERO_RPV
        , SUM(GAS_REWARD_REDEMPTIONS) / COUNT(DISTINCT HOUSEHOLD_ID) AS GAS_REDEMPTIONS_PER_VISITOR
  FROM GAS_BASE
GROUP BY all
""")

gas_agg_sp = spark.sql(gas_txns_agg)
# gas_agg_sp.display()

# COMMAND ----------

# DBTITLE 1,Category Aggregation
category_agg_query = (f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

EXPOSURE_TXNS AS(
SELECT e.*
    , COALESCE(COUNT(DISTINCT smic_category_id), 0) AS NUM_CATEGORIES
    , COALESCE(SUM(ITEM_QTY),0) AS ITEMS
FROM EXPOSURE_BASE as e
LEFT JOIN {category_table} as t
    ON e.HOUSEHOLD_ID = t.HOUSEHOLD_ID
    AND t.TXN_DTE BETWEEN '{EXP_START_DATE}' AND '{EXP_END_DATE}'
GROUP BY ALL
)

SELECT  VARIANT_ID
        , COUNT(DISTINCT {visitor_unit}) as VISITORS
        , COUNT(DISTINCT HOUSEHOLD_ID) as UNIQUE_HOUSEHOLDS
    -- Category Breadth
        , SUM(NUM_CATEGORIES) AS CATEGORIES_TOTAL
        , AVG(NUM_CATEGORIES) AS CATEGORIES_MEAN    -- Category Breadth
        , STDDEV(NUM_CATEGORIES) AS CATEGORIES_SD
    -- Items Per Category 
        , SUM(ITEMS) AS ITEMS_TOTAL
        , AVG(ITEMS) AS ITEMS_MEAN
        , STDDEV(ITEMS) AS ITEMS_SD
    --- Category Depth --- Similar to AOV
        , AVG(IFF(ITEMS > 0, ITEMS, NULL)) / AVG(IFF(NUM_CATEGORIES > 0, 
        NUM_CATEGORIES, NULL)) AS CATEGORY_DEPTH
        , COVAR_SAMP(ITEMS, NUM_CATEGORIES) AS CD_COV_ITEMS_CATEGORIES
FROM EXPOSURE_TXNS 
GROUP BY ALL
""")

category_agg_sp = spark.sql(category_agg_query)
#display(category_agg_df)

# COMMAND ----------

# DBTITLE 1,Account Health Aggregation
account_health_agg_query = (f"""
WITH
EXPOSURE_BASE AS(
      SELECT * FROM {exposure_table}
    ),

EXPOSURE_ACC_HEALTH AS (
   SELECT
       eh.*,
       ah.email_ind,
       ah.phone_ind,
       ah.fn_ln_ind,
       ah.bday_ind,
       ah.address_ind
   FROM EXPOSURE_BASE eh
   LEFT JOIN {account_health_table} ah
      ON eh.HOUSEHOLD_ID = ah.household_id
)

SELECT
   eah.VARIANT_ID, 
   COUNT(DISTINCT {visitor_unit}) as VISITORS,
   COUNT(DISTINCT eah.HOUSEHOLD_ID) as UNIQUE_HOUSEHOLDS,
   SUM(eah.email_ind) AS email_count,
   SUM(eah.phone_ind) AS phone_count,
   SUM(CASE WHEN eah.email_ind > 0 AND eah.phone_ind > 0 THEN 1 ELSE 0 END) AS reachablity_score,
   SUM(eah.fn_ln_ind) AS fn_ln_count,
   SUM(eah.bday_ind) AS bday_count,
   SUM(eah.address_ind) AS address_count,
   AVG(eah.email_ind + eah.phone_ind + eah.fn_ln_ind + eah.bday_ind + eah.address_ind) as health_score,
   STDDEV(eah.email_ind + eah.phone_ind + eah.fn_ln_ind + eah.bday_ind + eah.address_ind) as stdev_health_score
FROM EXPOSURE_ACC_HEALTH eah
GROUP BY ALL
""")

account_health_agg_sp = spark.sql(account_health_agg_query)
#display(account_health_agg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Margin Refresh and Fiscal Period Data Checks (***Deprecated***)

# COMMAND ----------

# DBTITLE 1,Margin Refresh Date
# margin_refresh_query = (
#   """
#   SELECT MAX(DATE(DATA_LOAD_DATE)) AS LAST_LOAD_DATE
#       , MAX(DATE(fw.FISCAL_WEEK_END_DT)) AS REFRESH_DATE
#   FROM gcp-abs-udco-bsvw-prod-prj-01.aamp_ds_datascience.EB_HH_STORE_WKLY_AGP_ALLDIV_VIEW as a
#   LEFT JOIN gcp-abs-udco-bqvw-prod-prj-01.udco_ds_acct.D0_FISCAL_WEEK AS fw
#             ON a.WEEK_ID = fw.FISCAL_WEEK_ID
#   WHERE fw.FISCAL_WEEK_END_DT >= (CURRENT_DATE()-60)
#   """
# )

# # Execute the query
# margin_refresh = bc.read_gcp_table(margin_refresh_query)
# margin_refresh_df = margin_refresh.select("*").toPandas()
# margin_refresh_date = margin_refresh_df['REFRESH_DATE'].iloc[0]
# margin_load_date = margin_refresh_df['LAST_LOAD_DATE'].iloc[0]
# print('Margin Valid Through:',margin_refresh_date, '\nLast Margin Load Date:', margin_load_date)

# COMMAND ----------

# DBTITLE 1,Fiscal Period Query
# fp_query = (
#   """
# SELECT FISCAL_PERIOD_NBR
#       , DATETIME(FISCAL_PERIOD_START_DT) AS FISCAL_PERIOD_START_DT
#       , DATETIME(FISCAL_PERIOD_END_DT) AS FISCAL_PERIOD_END_DT
# FROM gcp-abs-udco-bqvw-prod-prj-01.udco_ds_acct.D0_FISCAL_WEEK
# WHERE FISCAL_WEEK_START_DT BETWEEN (CURRENT_DATE()-7) AND CURRENT_DATE()
# ORDER BY 1
# """
# )

# # Execute the query
# current_fp = bc.read_gcp_table(fp_query)
# current_fp_df = current_fp.select("*").toPandas()

# try:
#   if margin_refresh_date > current_fp_df['FISCAL_PERIOD_START_DT'].iloc[0]:
#     next_margin_refresh_date  = str(current_fp_df['FISCAL_PERIOD_END_DT'].iloc[0] + datetime.timedelta(days=14))
#   else:
#     next_margin_refresh_date = str(current_fp_df['FISCAL_PERIOD_START_DT'].iloc[0] + datetime.timedelta(days=14))
# except:
#   next_margin_refresh_date = "NO MARGIN DATA AVALIABLE. PLEASE CHECK."
  
# print('Next Margin Refresh: ',next_margin_refresh_date)

# COMMAND ----------

# MAGIC %md
# MAGIC # Overall Statistics

# COMMAND ----------

# DBTITLE 1,Engagement (Click Hit Data)
base_df = agg_daily_sp.select("*").toPandas()
base_df = base_df.sort_values('VARIANT_ID')

base_df_for_display = base_df[['VARIANT_ID','VISITS_TOTAL','VISITS_MEAN','VISITORS','UNIQUE_HOUSEHOLDS','CART_ADDS_TOTAL','CART_ADDS_MEAN','CART_ADDS_CVR','UNITS_TOTAL','ORDERS_TOTAL','COUPON_CLIPS_TOTAL','COUPON_CLIPS_MEAN','COUPON_CLIP_CVR','SEARCHES_TOTAL','SEARCHES_MEAN','SEARCHES_CVR','REVENUE_TOTAL','AOV','UPO','RPV']].copy()

base_df_for_display ['AUTHENTICATED_RATE'] = base_df_for_display['UNIQUE_HOUSEHOLDS']/base_df_for_display['VISITORS']

base_df_for_display ['VISITORS'] = ['{:,}'.format(i) for i in base_df_for_display ['VISITORS']]
base_df_for_display ['UNIQUE_HOUSEHOLDS'] = ['{:,}'.format(i) for i in base_df_for_display ['UNIQUE_HOUSEHOLDS']]
base_df_for_display ['AUTHENTICATED_RATE'] = base_df_for_display ['AUTHENTICATED_RATE'].apply(lambda x: '{:,.4f}%'.format(x*100))
base_df_for_display ['VISITS_TOTAL'] = ['{:,}'.format(i) for i in base_df_for_display ['VISITS_TOTAL']]
base_df_for_display ['VISITS_PER_CUSTOMER'] = base_df_for_display['VISITS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
base_df_for_display ['CART_ADDS_TOTAL'] = base_df_for_display['CART_ADDS_TOTAL'].apply(lambda x: '{:,.0f}'.format(x))
base_df_for_display ['CART_ADDS_PER_CUSTOMER'] = base_df_for_display['CART_ADDS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
base_df_for_display ['CART_ADDS_CVR'] = base_df_for_display['CART_ADDS_CVR'].apply(lambda x: '{:,.4f}%'.format(x*100))
base_df_for_display ['UNITS_TOTAL'] = base_df_for_display['UNITS_TOTAL'].apply(lambda x: '{:,}'.format(x))
base_df_for_display ['ORDERS_TOTAL'] = base_df_for_display['ORDERS_TOTAL'].apply(lambda x: '{:,}'.format(x))
base_df_for_display ['COUPON_CLIPS_TOTAL'] = base_df_for_display['COUPON_CLIPS_TOTAL'].apply(lambda x: '{:,}'.format(x))
base_df_for_display ['COUPON_CLIPS_PER_CUSTOMER'] = base_df_for_display['COUPON_CLIPS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
base_df_for_display ['COUPON_CLIPS_CVR'] = base_df_for_display['COUPON_CLIP_CVR'].apply(lambda x: '{:,.4f}%'.format(x*100))
base_df_for_display ['SEARCHES_TOTAL'] = ['{:,}'.format(i) for i in base_df_for_display ['SEARCHES_TOTAL']]
base_df_for_display ['SEARCHES_PER_CUSTOMER'] = base_df_for_display['SEARCHES_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
base_df_for_display ['SEARCHES_CVR'] = base_df_for_display['SEARCHES_CVR'].apply(lambda x: '{:,.4f}%'.format(x*100))
base_df_for_display ['REVENUE_TOTAL'] = base_df_for_display['REVENUE_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
base_df_for_display ['AOV'] = base_df_for_display['AOV'].apply(lambda x: '${:,.4f}'.format(x))
base_df_for_display ['UPO'] = base_df_for_display['UPO'].apply(lambda x: '{:,.4f}'.format(x))
base_df_for_display ['RPV'] = base_df_for_display['RPV'].apply(lambda x: '${:,.4f}'.format(x))

display(base_df_for_display[['VARIANT_ID','VISITORS','UNIQUE_HOUSEHOLDS','AUTHENTICATED_RATE','VISITS_TOTAL','CART_ADDS_TOTAL','CART_ADDS_PER_CUSTOMER','CART_ADDS_CVR','COUPON_CLIPS_TOTAL','COUPON_CLIPS_PER_CUSTOMER','COUPON_CLIPS_CVR','SEARCHES_TOTAL','SEARCHES_PER_CUSTOMER','SEARCHES_CVR']])

# COMMAND ----------

# DBTITLE 1,Margin (Weekly)
try:
  margin_agg_df = margin_agg_sp.select("*").toPandas()
  margin_agg_df = margin_agg_df.sort_values('VARIANT_ID',ascending=True)
      
  margin_df_for_display = margin_agg_df[['VARIANT_ID','VISITORS','MARGIN_TOTAL','MARGIN_MEAN']].copy()

  margin_df_for_display ['CUSTOMERS'] = ['{:,}'.format(i) for i in margin_df_for_display ['VISITORS']]
  margin_df_for_display ['MARGIN_TOTAL'] = margin_df_for_display['MARGIN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
  margin_df_for_display ['MARGIN_PER_CUSTOMER'] = margin_df_for_display['MARGIN_MEAN'].apply(lambda x: '${:,.2f}'.format(x))

  display(margin_df_for_display[['VARIANT_ID','MARGIN_TOTAL','MARGIN_PER_CUSTOMER']])
except:
  print("Margin is not computed for this test.  Please check the last time that margin was updated.")

# COMMAND ----------

# DBTITLE 1,AGP
agp_agg_df = agp_agg_sp.select("*").toPandas()
agp_agg_df = agp_agg_df.sort_values('VARIANT_ID',ascending=True)

agp_df_for_display = agp_agg_df[['VARIANT_ID','VISITORS','AGP_TOTAL','AGP_MEAN','NET_SALES_TOTAL','NET_SALES_MEAN']].copy()

agp_df_for_display ['CUSTOMERS'] = ['{:,}'.format(i) for i in agp_df_for_display ['VISITORS']]
agp_df_for_display ['AGP_TOTAL'] = agp_df_for_display['AGP_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
agp_df_for_display ['AGP_PER_CUSTOMER'] = agp_df_for_display['AGP_MEAN'].apply(lambda x: '${:,.2f}'.format(x))
agp_df_for_display ['NET_SALES_TOTAL'] = agp_df_for_display['NET_SALES_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
agp_df_for_display ['NET_SALES_PER_CUSTOMER'] = agp_df_for_display['NET_SALES_MEAN'].apply(lambda x: '${:,.2f}'.format(x))

display(agp_df_for_display[['VARIANT_ID','AGP_TOTAL','AGP_PER_CUSTOMER','NET_SALES_TOTAL','NET_SALES_PER_CUSTOMER']])

# COMMAND ----------

# DBTITLE 1,eComm TXNs and Redemptions
# Convert the Spark DataFrame to a Pandas DataFrame and sort
ecomm_agg_df = ecomm_agg_sp.select("*").toPandas()
ecomm_agg_df = ecomm_agg_df.sort_values('VARIANT_ID')

# Select columns for display
ecomm_df_for_display = ecomm_agg_df[[
   'VARIANT_ID','VISITORS','PURCHASING_CUSTOMERS','ECOMM_UNITS_TOTAL',
   'ECOMM_ORDERS_TOTAL','ECOMM_REVENUE_TOTAL', 'ECOMM_NET_SALES_TOTAL',
   'ECOMM_REVENUE_NONZERO_MEAN', 'ECOMM_NET_SALES_MEAN', 'ECOMM_NET_SALES_NONZERO_MEAN',
   'ECOMM_BNC_TOTAL','ECOMM_AOV','ECOMM_UPO','ECOMM_RPV','REDEEMING_COUNT_ECOMM',
   'ECOMM_REDEMPTIONS_TOTAL','ECOMM_MKDN_TOTAL','ECOMM_REDEMPTIONS_MEAN','ECOMM_MKDN_MEAN'
]].copy()

# --- Metric Calculation and Formatting ---
ecomm_df_for_display ['ECOMM_CVR'] = ecomm_df_for_display['PURCHASING_CUSTOMERS']/ecomm_df_for_display['VISITORS']
ecomm_df_for_display ['ECOMM_BNC_CVR'] = ecomm_df_for_display['ECOMM_BNC_TOTAL']/ecomm_df_for_display['VISITORS']
ecomm_df_for_display ['PURCHASING_CUSTOMERS'] = ['{:,}'.format(i) for i in ecomm_df_for_display ['PURCHASING_CUSTOMERS']]
ecomm_df_for_display ['ECOMM_UNITS_TOTAL'] = ['{:,}'.format(i) for i in ecomm_df_for_display ['ECOMM_UNITS_TOTAL']]
ecomm_df_for_display ['ECOMM_TXNS_TOTAL'] = ['{:,}'.format(i) for i in ecomm_df_for_display ['ECOMM_ORDERS_TOTAL']]
ecomm_df_for_display ['UNIQUE_REEDEMERS_ECOMM'] = ['{:,}'.format(i) for i in ecomm_df_for_display ['REDEEMING_COUNT_ECOMM']]
ecomm_df_for_display ['ECOMM_REDEMPTIONS_TOTAL'] = ['{:,}'.format(i) for i in ecomm_df_for_display ['ECOMM_REDEMPTIONS_TOTAL']]
ecomm_df_for_display ['ECOMM_REVENUE_TOTAL'] = ecomm_df_for_display['ECOMM_REVENUE_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
ecomm_df_for_display ['ECOMM_MKDN_TOTAL'] = ecomm_df_for_display['ECOMM_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
ecomm_df_for_display ['ECOMM_NET_SALES_TOTAL'] = ecomm_df_for_display['ECOMM_NET_SALES_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
ecomm_df_for_display ['ECOMM_AOV'] = ecomm_df_for_display['ECOMM_AOV'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_UPO'] = ecomm_df_for_display['ECOMM_UPO'].apply(lambda x: '{:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_REVENUE_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_RPV'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_NONZERO_REVENUE_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_REVENUE_NONZERO_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_REDEMPTIONS_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_REDEMPTIONS_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_MKDN_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_MKDN_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_NET_SALES_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_NET_SALES_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display ['ECOMM_NONZERO_NET_SALES_PER_CUSTOMER'] = ecomm_df_for_display['ECOMM_NET_SALES_NONZERO_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
ecomm_df_for_display['ECOMM_CVR'] = ecomm_df_for_display['ECOMM_CVR'].apply(lambda x: '{:,.4f}'.format(x))

# --- Final Display ---
display(ecomm_df_for_display[[
   'VARIANT_ID',
   'PURCHASING_CUSTOMERS',
   'ECOMM_UNITS_TOTAL',
   'ECOMM_TXNS_TOTAL',
   'ECOMM_REVENUE_TOTAL',
   'ECOMM_NET_SALES_TOTAL',
   'ECOMM_BNC_TOTAL',
   'ECOMM_BNC_CVR',
   'ECOMM_AOV',
   'ECOMM_UPO',
   'ECOMM_REVENUE_PER_CUSTOMER',
   'ECOMM_NONZERO_REVENUE_PER_CUSTOMER',
   'ECOMM_NET_SALES_PER_CUSTOMER',
   'ECOMM_NONZERO_NET_SALES_PER_CUSTOMER', 
   'UNIQUE_REEDEMERS_ECOMM',
   'ECOMM_REDEMPTIONS_TOTAL',
   'ECOMM_MKDN_TOTAL',
   'ECOMM_CVR'
]])

# COMMAND ----------

# DBTITLE 1,In-Store TXNs and Redemptions
store_agg_df = store_agg_sp.select("*").toPandas()
store_agg_df = store_agg_df.sort_values('VARIANT_ID')

store_df_for_display = store_agg_df[['VARIANT_ID','PURCHASING_CUSTOMERS','STORE_UNITS_TOTAL','STORE_ORDERS_TOTAL','STORE_REVENUE_TOTAL','STORE_REVENUE_MEAN','STORE_REVENUE_NONZERO_MEAN','STORE_AOV','STORE_UPO','REDEEMING_COUNT_STORE','STORE_REDEMPTIONS_TOTAL','STORE_MKDN_TOTAL']].copy()

store_df_for_display ['PURCHASING_CUSTOMERS'] = ['{:,}'.format(i) for i in store_df_for_display ['PURCHASING_CUSTOMERS']]
store_df_for_display ['STORE_UNITS_TOTAL'] = ['{:,}'.format(i) for i in store_df_for_display ['STORE_UNITS_TOTAL']]
store_df_for_display ['STORE_TXNS_TOTAL'] = ['{:,}'.format(i) for i in store_df_for_display ['STORE_ORDERS_TOTAL']]
store_df_for_display ['STORE_REVENUE_TOTAL'] = store_df_for_display['STORE_REVENUE_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
store_df_for_display ['STORE_AOV'] = store_df_for_display['STORE_AOV'].apply(lambda x: '${:,.4f}'.format(x))
store_df_for_display ['STORE_UPO'] = store_df_for_display['STORE_UPO'].apply(lambda x: '{:,.4f}'.format(x))
store_df_for_display ['STORE_REVENUE_PER_CUSTOMER'] = store_df_for_display['STORE_REVENUE_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
store_df_for_display ['UNIQUE_REDEEMERS_STORE'] = ['{:,}'.format(i) for i in store_df_for_display ['REDEEMING_COUNT_STORE']]
store_df_for_display ['STORE_REDEMPTIONS_TOTAL'] = ['{:,}'.format(i) for i in store_df_for_display ['STORE_REDEMPTIONS_TOTAL']]
store_df_for_display ['STORE_MKDN_TOTAL'] = store_df_for_display['STORE_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
store_df_for_display ['STORE_NONZERO_REVENUE_PER_CUSTOMER'] = store_df_for_display['STORE_REVENUE_NONZERO_MEAN'].apply(lambda x: '${:,.4f}'.format(x))

display(store_df_for_display[['VARIANT_ID','PURCHASING_CUSTOMERS','STORE_UNITS_TOTAL','STORE_TXNS_TOTAL','STORE_REVENUE_TOTAL','STORE_AOV','STORE_UPO','STORE_REVENUE_PER_CUSTOMER','STORE_NONZERO_REVENUE_PER_CUSTOMER','UNIQUE_REDEEMERS_STORE','STORE_REDEMPTIONS_TOTAL','STORE_MKDN_TOTAL']])

# COMMAND ----------

# DBTITLE 1,Gas TXNs and Redemptions
try:
  gas_agg_df = gas_agg_sp.select("*").toPandas()
  gas_agg_df = gas_agg_df.sort_values('VARIANT_ID_GAS')
  
  gas_df_for_display = gas_agg_df[['VARIANT_ID_GAS','GAS_VISITORS','GAS_ORDERS_TOTAL','GAS_REVENUE_TOTAL','GAS_REVENUE_NONZERO_MEAN','GAS_AOV','GAS_RPV','GAS_MKDN_MEAN','GAS_REWARD_REDEMPTIONS_TOTAL']].copy()

  gas_df_for_display ['PURCHASING_GAS_VISITORS'] = ['{:,}'.format(i) for i in gas_df_for_display ['GAS_VISITORS']]
  gas_df_for_display ['GAS_TXNS_TOTAL'] = ['{:,}'.format(i) for i in gas_df_for_display ['GAS_ORDERS_TOTAL']]
  gas_df_for_display ['GAS_REVENUE_TOTAL'] = gas_df_for_display['GAS_REVENUE_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
  gas_df_for_display ['GAS_AOV'] = gas_df_for_display['GAS_AOV'].apply(lambda x: '${:,.4f}'.format(x))
  gas_df_for_display ['GAS_REVENUE_PER_CUSTOMER'] = gas_df_for_display['GAS_RPV'].apply(lambda x: '${:,.4f}'.format(x))
  gas_df_for_display ['GAS_MKDN_PER_CUSTOMER'] = gas_df_for_display['GAS_MKDN_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
  gas_df_for_display ['GAS_REWARD_REDEMPTIONS_TOTAL'] = ['{:,}'.format(i) for i in gas_df_for_display ['GAS_REWARD_REDEMPTIONS_TOTAL']]

  display(gas_df_for_display[['VARIANT_ID_GAS','PURCHASING_GAS_VISITORS','GAS_TXNS_TOTAL','GAS_REVENUE_TOTAL','GAS_AOV','GAS_REVENUE_PER_CUSTOMER','GAS_MKDN_PER_CUSTOMER','GAS_REWARD_REDEMPTIONS_TOTAL']])
except:
  display("NO GAS TRANSACTION DATA.")

# COMMAND ----------

# DBTITLE 1,Combined (Ecomm and In-Store) TXNs and Redemptions
combined_df = txns_sp.select("*").toPandas()
combined_df = combined_df.sort_values(['VARIANT_ID'],ascending=True)
combined_for_display = combined_df.copy()

combined_for_display['CVR'] = combined_for_display['PURCHASING_CUSTOMERS']/combined_for_display['VISITORS']
combined_for_display ['VISITORS'] = ['{:,}'.format(i) for i in combined_for_display ['VISITORS']]
combined_for_display ['PURCHASING_CUSTOMERS'] = ['{:,}'.format(i) for i in combined_for_display ['PURCHASING_CUSTOMERS']]
combined_for_display ['COMBINED_REVENUE_TOTAL'] = combined_for_display['COMBINED_REVENUE_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
combined_for_display ['COMBINED_REVENUE_PER_CUSTOMER'] = combined_for_display['COMBINED_RPV'].apply(lambda x: '${:,.4f}'.format(x))
combined_for_display ['TXNS_TOTAL'] = ['{:,}'.format(i) for i in combined_for_display ['COMBINED_ORDERS_TOTAL']]
combined_for_display ['TXNS_PER_CUSTOMER'] = combined_for_display['COMBINED_ORDERS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
combined_for_display ['UNITS_TOTAL'] = ['{:,}'.format(i) for i in combined_for_display ['COMBINED_UNITS_TOTAL']]
combined_for_display ['UNITS_PER_CUSTOMER'] = combined_for_display['COMBINED_UNITS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
combined_for_display ['AOV'] = combined_for_display['COMBINED_AOV'].apply(lambda x: '${:,.4f}'.format(x))
combined_for_display ['UPO'] = combined_for_display['COMBINED_UPO'].apply(lambda x: '{:,.4f}'.format(x))
combined_for_display ['COMBINED_NONZERO_REVENUE_PER_CUSTOMER'] = combined_for_display['COMBINED_REVENUE_NONZERO_MEAN'].apply(lambda x: '${:,.4f}'.format(x))
combined_for_display['CVR'] = combined_for_display['CVR'].apply(lambda x: '{:,.4f}'.format(x))


display(combined_for_display[['VARIANT_ID','COMBINED_REVENUE_TOTAL','CVR','COMBINED_REVENUE_PER_CUSTOMER','COMBINED_NONZERO_REVENUE_PER_CUSTOMER','TXNS_TOTAL','UNITS_TOTAL','UNITS_PER_CUSTOMER']])

# COMMAND ----------

# DBTITLE 1,Clips Breakdown
clips_df = clips_agg_sp.select("*").toPandas()
clips_df = clips_df.sort_values('VARIANT_ID', ascending=True)
clips_for_display = clips_df[['VARIANT_ID','VISITORS','UNIQUE_CLIPPING_HH','CLIPS_TOTAL','CLIPS_MEAN','pd_clips_TOTAL','gr_clips_TOTAL','mf_clips_TOTAL', 'spd_clips_TOTAL', 'pzn_clips_TOTAL', 'sc_clips_TOTAL']].copy()

clips_for_display['% CLIPPING'] = 100*clips_for_display ['UNIQUE_CLIPPING_HH'] / clips_for_display ['VISITORS']

# clips_for_display ['VISITORS'] = ['{:,}'.format(i) for i in clips_for_display ['VISITORS']]
clips_for_display ['UNIQUE_CLIPPING_HH'] = ['{:,}'.format(i) for i in clips_for_display ['UNIQUE_CLIPPING_HH']]
clips_for_display ['% CLIPPING'] = clips_for_display['% CLIPPING'].apply(lambda x: '{:,.4f}'.format(x*100))

clips_for_display ['CLIPS_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['CLIPS_TOTAL']]
clips_for_display ['CLIPS_PER_CUSTOMER'] = clips_for_display['CLIPS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
clips_for_display ['pd_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['pd_clips_TOTAL']]
clips_for_display ['gr_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['gr_clips_TOTAL']]
clips_for_display ['mf_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['mf_clips_TOTAL']]
clips_for_display ['sc_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['sc_clips_TOTAL']]
clips_for_display ['spd_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['spd_clips_TOTAL']]
clips_for_display ['pzn_clips_TOTAL'] = ['{:,}'.format(i) for i in clips_for_display ['pzn_clips_TOTAL']]

display(clips_for_display[['VARIANT_ID','UNIQUE_CLIPPING_HH','% CLIPPING','CLIPS_TOTAL','CLIPS_PER_CUSTOMER','pd_clips_TOTAL','gr_clips_TOTAL','mf_clips_TOTAL', 'spd_clips_TOTAL',
       'pzn_clips_TOTAL', 'sc_clips_TOTAL']])

# COMMAND ----------

# DBTITLE 1,Redemptions Breakdown
redemptions_df = redemptions_sp.select("*").toPandas()
redemptions_df = redemptions_df.sort_values('VARIANT_ID', ascending=True)
redemptions_for_display = redemptions_df[['VARIANT_ID','VISITORS','REDEEMING_COUNT_COMBINED','REDEMPTIONS_TOTAL','REDEMPTIONS_MEAN','pd_redemptions_TOTAL','gr_redemptions_TOTAL','mf_redemptions_TOTAL', 'spd_redemptions_TOTAL','pzn_redemptions_TOTAL', 'sc_redemptions_TOTAL','MKDN_TOTAL','MKDN_MEAN','pd_MKDN_TOTAL', 'gr_MKDN_TOTAL', 'mf_MKDN_TOTAL', 'spd_MKDN_TOTAL', 'pzn_MKDN_TOTAL', 'sc_MKDN_TOTAL','BP_EARNING_HOUSEHOLDS','BONUS_POINTS_TOTAL','ESTIMATED_MKDN_TOTAL']].copy()
       
redemptions_for_display['% REDEEMING'] = redemptions_for_display ['REDEEMING_COUNT_COMBINED'] / redemptions_for_display ['VISITORS']
redemptions_for_display ['UNIQUE_REDEEMERS'] = ['{:,}'.format(i) for i in redemptions_for_display ['REDEEMING_COUNT_COMBINED']]
redemptions_for_display ['% REDEEMING'] = redemptions_for_display['% REDEEMING'].apply(lambda x: '{:,.4f}'.format(x))

redemptions_for_display['% BP EARNING'] = redemptions_for_display ['BP_EARNING_HOUSEHOLDS'] / redemptions_for_display ['VISITORS']
redemptions_for_display ['UNIQUE_BP_EARNING_HH'] = ['{:,}'.format(i) for i in redemptions_for_display ['BP_EARNING_HOUSEHOLDS']]
redemptions_for_display ['% BP EARNING'] = redemptions_for_display['% BP EARNING'].apply(lambda x: '{:,.4f}'.format(x))

redemptions_for_display ['REDEMPTIONS_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['REDEMPTIONS_TOTAL']]
redemptions_for_display ['REDEMPTIONS_PER_CUSTOMER'] = redemptions_for_display['REDEMPTIONS_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
redemptions_for_display ['MKDN_PER_CUSTOMER'] = redemptions_for_display['MKDN_MEAN'].apply(lambda x: '${:,.4f}'.format(x))

redemptions_for_display ['pd_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['pd_redemptions_TOTAL']]
redemptions_for_display ['gr_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['gr_redemptions_TOTAL']]
redemptions_for_display ['mf_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['mf_redemptions_TOTAL']]
redemptions_for_display ['sc_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['sc_redemptions_TOTAL']]
redemptions_for_display ['spd_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['spd_redemptions_TOTAL']]
redemptions_for_display ['pzn_redemptions_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['pzn_redemptions_TOTAL']]
redemptions_for_display ['BONUS_POINTS_TOTAL'] = ['{:,}'.format(i) for i in redemptions_for_display ['BONUS_POINTS_TOTAL']]

redemptions_for_display['MKDN_TOTAL'] = redemptions_for_display['MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['pd_MKDN_TOTAL'] = redemptions_for_display['pd_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['gr_MKDN_TOTAL'] = redemptions_for_display['gr_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['mf_MKDN_TOTAL'] = redemptions_for_display['mf_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['sc_MKDN_TOTAL'] = redemptions_for_display['sc_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['spd_MKDN_TOTAL'] = redemptions_for_display['spd_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['pzn_MKDN_TOTAL'] = redemptions_for_display['pzn_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))
redemptions_for_display['ESTIMATED_MKDN_TOTAL'] = redemptions_for_display['ESTIMATED_MKDN_TOTAL'].apply(lambda x: '${:,.0f}'.format(x))

display(redemptions_for_display[['VARIANT_ID','UNIQUE_REDEEMERS','% REDEEMING','REDEMPTIONS_TOTAL','pd_redemptions_TOTAL','gr_redemptions_TOTAL','mf_redemptions_TOTAL', 'spd_redemptions_TOTAL',
       'pzn_redemptions_TOTAL', 'sc_redemptions_TOTAL','% BP EARNING','BONUS_POINTS_TOTAL','ESTIMATED_MKDN_TOTAL']])

# COMMAND ----------

# DBTITLE 1,Markdown Breakdown
markdown_for_display = redemptions_for_display.merge(store_df_for_display[['VARIANT_ID','STORE_MKDN_TOTAL']],on='VARIANT_ID', how='left')
markdown_for_display = markdown_for_display.merge(ecomm_df_for_display[['VARIANT_ID','ECOMM_MKDN_TOTAL']],on='VARIANT_ID', how='left')

display(markdown_for_display[['VARIANT_ID','MKDN_TOTAL','ECOMM_MKDN_TOTAL','STORE_MKDN_TOTAL','pd_MKDN_TOTAL', 'gr_MKDN_TOTAL', 'mf_MKDN_TOTAL', 'spd_MKDN_TOTAL', 'pzn_MKDN_TOTAL', 'sc_MKDN_TOTAL']])

# COMMAND ----------

# DBTITLE 1,Basket Health
basket_health_df = basket_health_agg_sp.select("*").toPandas()
basket_health_df = basket_health_df.sort_values('VARIANT_ID', ascending=True)
bh_for_display = basket_health_df[['VARIANT_ID','VISITORS','UNIQUE_BASKET_HEALTH_HH','BASKET_RATE_TOTAL','BASKET_RATE_MEAN','AB_RATE_TOTAL','AB_RATE_MEAN','DE_RATE_TOTAL', 'DE_RATE_MEAN', 'TOTAL_BASKET_HEALTH_TXNS']].copy()
bh_for_display['% BASKET HEALTH HH'] = bh_for_display ['UNIQUE_BASKET_HEALTH_HH'] / bh_for_display ['VISITORS']
bh_for_display['% BASKET TXNS'] = bh_for_display ['TOTAL_BASKET_HEALTH_TXNS'] / combined_df ['COMBINED_ORDERS_TOTAL']

bh_for_display ['UNIQUE_BASKET_HEALTH_HH'] = ['{:,}'.format(i) for i in bh_for_display ['UNIQUE_BASKET_HEALTH_HH']]
bh_for_display ['% BASKET HEALTH HH'] = bh_for_display ['% BASKET HEALTH HH'].apply(lambda x: '{:,.4f}'.format(x*100))

bh_for_display ['NET_BASKET_RATE_TOTAL'] = bh_for_display['BASKET_RATE_TOTAL'].apply(lambda x: '{:,.4f}'.format(x))
bh_for_display ['NET_BASKET_RATE_MEAN'] = bh_for_display['BASKET_RATE_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
bh_for_display ['AB_RATE_MEAN'] = bh_for_display ['AB_RATE_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
bh_for_display ['DE_RATE_MEAN'] = bh_for_display['DE_RATE_MEAN'].apply(lambda x: '{:,.4f}'.format(x))
bh_for_display ['TOTAL_BASKET_HEALTH_TXNS'] = ['{:,}'.format(i) for i in bh_for_display ['TOTAL_BASKET_HEALTH_TXNS']]
bh_for_display ['% BASKET TXNS'] = bh_for_display['% BASKET TXNS'].apply(lambda x: '{:,.4f}'.format(x*100))

display(bh_for_display[['VARIANT_ID','UNIQUE_BASKET_HEALTH_HH','% BASKET HEALTH HH','NET_BASKET_RATE_TOTAL','NET_BASKET_RATE_MEAN','AB_RATE_MEAN','DE_RATE_MEAN','TOTAL_BASKET_HEALTH_TXNS', '% BASKET TXNS']])


# COMMAND ----------

# DBTITLE 1,Category Depth And Breadth
category_breadth_depth_df = category_agg_sp.select("*").toPandas()
category_breadth_depth_df = category_breadth_depth_df.sort_values('VARIANT_ID', ascending=True)

category_breadth_depth_df['CATEGORIES_TOTAL'] = category_breadth_depth_df['CATEGORIES_TOTAL'].apply(lambda x: '{:,.0f}'.format(x))
category_breadth_depth_df['ITEMS_TOTAL'] = category_breadth_depth_df['ITEMS_TOTAL'].apply(lambda x: '{:,.0f}'.format(x))

for col in ['CATEGORIES_MEAN', 'CATEGORIES_SD', 'ITEMS_MEAN', 'ITEMS_SD', 'CATEGORY_DEPTH', 'CD_COV_ITEMS_CATEGORIES']:
    category_breadth_depth_df[col] = category_breadth_depth_df[col].astype(str).str.replace(',', '').astype(float)
    category_breadth_depth_df[col] = category_breadth_depth_df[col].round(4)

display(category_breadth_depth_df)

# COMMAND ----------

# DBTITLE 1,Account Health
account_health_agg_df = account_health_agg_sp.select("*").toPandas()
account_health_agg_df = account_health_agg_df.sort_values('VARIANT_ID', ascending=True)

account_health_display = account_health_agg_df[['VARIANT_ID','UNIQUE_HOUSEHOLDS','email_count', 'phone_count', 'fn_ln_count', 'bday_count', 'address_count']].copy()
columns_to_format = ['UNIQUE_HOUSEHOLDS','email_count', 'phone_count', 'fn_ln_count', 'bday_count', 'address_count']
account_health_display[columns_to_format] = account_health_display[columns_to_format].applymap(lambda x: f"{x:,}")
display(account_health_display)

# COMMAND ----------

# DBTITLE 1,Output Table
summary_table1 = base_df_for_display[['VARIANT_ID','VISITORS','UNIQUE_HOUSEHOLDS','AUTHENTICATED_RATE','VISITS_TOTAL','VISITS_PER_CUSTOMER','SEARCHES_TOTAL','SEARCHES_PER_CUSTOMER','SEARCHES_CVR','CART_ADDS_TOTAL','CART_ADDS_PER_CUSTOMER','CART_ADDS_CVR','COUPON_CLIPS_TOTAL','COUPON_CLIPS_PER_CUSTOMER','COUPON_CLIPS_CVR']].merge(
    combined_for_display[['VARIANT_ID','UNITS_TOTAL','UNITS_PER_CUSTOMER','TXNS_TOTAL','TXNS_PER_CUSTOMER','COMBINED_REVENUE_TOTAL','COMBINED_REVENUE_PER_CUSTOMER','COMBINED_NONZERO_REVENUE_PER_CUSTOMER','CVR','AOV','UPO']],
    on='VARIANT_ID', how='left'
).merge(
    redemptions_for_display[['VARIANT_ID','REDEMPTIONS_TOTAL','REDEMPTIONS_PER_CUSTOMER','MKDN_TOTAL','MKDN_PER_CUSTOMER']],
    on='VARIANT_ID', how='left'
)


summary_table2 = summary_table1.merge(ecomm_df_for_display[['VARIANT_ID','ECOMM_REVENUE_TOTAL','ECOMM_REVENUE_PER_CUSTOMER','ECOMM_NONZERO_REVENUE_PER_CUSTOMER','ECOMM_AOV', 'ECOMM_UPO','ECOMM_REDEMPTIONS_TOTAL','ECOMM_MKDN_TOTAL','ECOMM_REDEMPTIONS_PER_CUSTOMER', 'ECOMM_MKDN_PER_CUSTOMER','ECOMM_CVR','ECOMM_BNC_TOTAL','ECOMM_BNC_CVR']],on='VARIANT_ID', how='left')

summary_table3 = summary_table2.merge(margin_df_for_display[['VARIANT_ID','MARGIN_TOTAL','MARGIN_PER_CUSTOMER']], on='VARIANT_ID', how='left')

summary_table4 = summary_table3.merge(agp_df_for_display[['VARIANT_ID','AGP_TOTAL','AGP_PER_CUSTOMER','NET_SALES_TOTAL','NET_SALES_PER_CUSTOMER']], on='VARIANT_ID', how='left')

summary_table5 = summary_table4.merge(category_breadth_depth_df[['VARIANT_ID','CATEGORIES_MEAN','CATEGORY_DEPTH']], on='VARIANT_ID', how='left')

output_table = summary_table5[['VARIANT_ID','VISITORS','COMBINED_REVENUE_PER_CUSTOMER','COMBINED_NONZERO_REVENUE_PER_CUSTOMER','NET_SALES_PER_CUSTOMER','ECOMM_REVENUE_PER_CUSTOMER','MKDN_PER_CUSTOMER','AGP_PER_CUSTOMER','CVR', 'AOV', 'UPO', 'TXNS_PER_CUSTOMER', 'UNITS_PER_CUSTOMER','VISITS_PER_CUSTOMER','SEARCHES_PER_CUSTOMER','CART_ADDS_PER_CUSTOMER','COUPON_CLIPS_PER_CUSTOMER','REDEMPTIONS_PER_CUSTOMER','SEARCHES_CVR','CART_ADDS_CVR','COUPON_CLIPS_CVR','VISITS_TOTAL','SEARCHES_TOTAL','CART_ADDS_TOTAL','COUPON_CLIPS_TOTAL','TXNS_TOTAL','UNITS_TOTAL','COMBINED_REVENUE_TOTAL','NET_SALES_TOTAL','MKDN_TOTAL','REDEMPTIONS_TOTAL','ECOMM_REVENUE_TOTAL', 'ECOMM_MKDN_TOTAL', 'ECOMM_REDEMPTIONS_TOTAL', 'ECOMM_NONZERO_REVENUE_PER_CUSTOMER', 'ECOMM_REDEMPTIONS_PER_CUSTOMER', 'ECOMM_MKDN_PER_CUSTOMER','ECOMM_CVR','ECOMM_BNC_TOTAL','ECOMM_BNC_CVR','CATEGORIES_MEAN','CATEGORY_DEPTH','UNIQUE_HOUSEHOLDS','AUTHENTICATED_RATE']]

output_table = output_table.rename(columns={"CATEGORIES_MEAN": "CATEGORY_BREADTH"})

display(output_table)

# COMMAND ----------

# DBTITLE 1,Summary Table
## TODO - We may be able to delete this cell
summary_table4 = summary_table4[['VARIANT_ID','UNIQUE_HOUSEHOLDS','COMBINED_REVENUE_PER_CUSTOMER','COMBINED_NONZERO_REVENUE_PER_CUSTOMER','NET_SALES_PER_CUSTOMER','CVR','MKDN_PER_CUSTOMER','AGP_PER_CUSTOMER','TXNS_PER_CUSTOMER','UNITS_PER_CUSTOMER','VISITS_PER_CUSTOMER','SEARCHES_PER_CUSTOMER','CART_ADDS_PER_CUSTOMER','COUPON_CLIPS_PER_CUSTOMER','MARGIN_PER_CUSTOMER','REDEMPTIONS_PER_CUSTOMER']]

display(summary_table4)

# COMMAND ----------

if WINSORIZE != 'OFF':

  if round(ecomm_agg_df['WINSORIZATION_THRESHOLD'].iloc[0],2) == 0:
    ecomm_thresh = 'NO WINSORIZATION DUE TO $0.00 AT 99th'
    ecomm_win_cnt = 0
    ecomm_win_pct = 0
  else:
    ecomm_thresh = f'''${round(ecomm_agg_df['WINSORIZATION_THRESHOLD'].iloc[0],2)}'''
    ecomm_win_cnt = ecomm_agg_df['WINSORIZED_CUSTOMERS']
    ecomm_win_pct = round(ecomm_agg_df['WINSORIZED_CUSTOMERS']/ecomm_agg_df['VISITORS']*100.00,3)
  
  if round(store_agg_df['WINSORIZATION_THRESHOLD'].iloc[0],2) == 0:
    store_thresh = 'NO WINSORIZATION DUE TO $0.00 AT 99th'
    store_win_cnt = 0
    store_win_pct = 0
  else:
    store_thresh = f'''${round(store_agg_df['WINSORIZATION_THRESHOLD'].iloc[0],2)}'''
    store_win_cnt = store_agg_df['WINSORIZED_CUSTOMERS']
    store_win_pct = round(store_agg_df['WINSORIZED_CUSTOMERS']/store_agg_df['VISITORS']*100.00,3)
  
  if round(combined_df['WINSORIZATION_THRESHOLD'].iloc[0],2) == 0:
    combined_thresh = 'NO WINSORIZATION DUE TO $0.00 AT 99th'
    combined_win_cnt = 0
    combined_win_pct = 0
  else:
    combined_thresh = f'''${round(combined_df['WINSORIZATION_THRESHOLD'].iloc[0],2)}'''
    combined_win_cnt = combined_df['WINSORIZED_CUSTOMERS']
    combined_win_pct = round(combined_df['WINSORIZED_CUSTOMERS']/combined_df['VISITORS']*100.00,3)
  
  displayHTML(f"""<h3><font color="black"> Winsorized (99th Percentile) Values: </font></h3> 
                  <p><font color="black"> EComm 99th:  {ecomm_thresh} </font></p>
                  <p><font color="black"> In-Store 99th:  {store_thresh} </font></p>
                  <p><font color="black"> Combined 99th:  {combined_thresh} </font></p>
                  """)
else:
    displayHTML(f"""<h3><font color="black"> Winsorization is turned OFF for this experiment analysis. </font></h3> 
    """)

# COMMAND ----------

if WINSORIZE != 'OFF':
  win_df_ecomm = pd.DataFrame({
    "Variant": ecomm_agg_df['VARIANT_ID'],
    "EComm Txns Winzorized Count": ecomm_win_cnt,
    "Ecomm Txns Winzorized Percentage": ecomm_win_pct
  })

  win_df_store = pd.DataFrame({
    "Variant": store_agg_df['VARIANT_ID'],
    "In-Store Txns Winzorized Count": store_win_cnt,
    "In-Store Txns Winzorized Percentage": store_win_pct
  })

  win_df_combined = pd.DataFrame({
    "Variant": combined_df['VARIANT_ID'],
    "Combined Txns Winzorized Count": combined_win_cnt,
    "Combined Txns Winzorized Percentage": combined_win_pct
  })

  display(win_df_combined.merge(win_df_store, on = 'Variant').merge(win_df_ecomm, on = 'Variant'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Experiment Details & Health Checks

# COMMAND ----------

# DBTITLE 1,Experiment Details
current_time = str(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))

if PAGE_FILTER_INPUT:
  page_filter_display = PAGE_FILTER_INPUT.upper()
  page_filter_link = PAGE_FILTER_INPUT.replace(", ","_").replace(",","_").replace(" ","_")
else:
  page_filter_display = 'No_Filter'
  page_filter_link = 'No_Filter'

db_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
host_name = spark.conf.get("spark.databricks.workspaceUrl")
if 'gcp' in host_name.lower():
  output_table_link = f"https://{db_id}.1.gcp.databricks.com/files/SAFE/{EXPERIMENT_ID}_{page_filter_link}_{OS_PLATFORM}_{current_time}.csv?o={db_id}"
else:
  output_table_link = f"https://adb-{db_id}.12.azuredatabricks.net/files/SAFE/{EXPERIMENT_ID}_{page_filter_link}_{OS_PLATFORM}_{current_time}.csv?o={db_id}"

output_table.to_csv(f"""/dbfs/FileStore/SAFE/{EXPERIMENT_ID}_{page_filter_link}_{OS_PLATFORM}_{current_time}.csv""",index=False)

displayHTML(f"""<h3><font color="grey"> Experiment: {EXPERIMENT_ID} conducted from {EXP_START_DATE} to {EXP_END_DATE} </font></h3> 
          <h3><font color="grey"> Comparing variations: {list(base_df['VARIANT_ID'])} </font></h3> 
          <h3><font color="grey"> Page filter: {page_filter_display} </font></h3> 
          <h3><font color="grey"> Exposure filter: {EXPOSURE_FILTER} </font></h3> 
          <h3><font color="grey"> App filter: {OS_PLATFORM} </font></h3> 
          <h3><font color="grey"> Detailed Output Table: {output_table_link} </font></h3>
           """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary Metrics (In-Store + Ecomm)

# COMMAND ----------

# DBTITLE 1,Customers
# This test is for final DataFrame
visitors_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "VISITORS",
    std_column_name = "VISITS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(visitors_test[0])

# COMMAND ----------

# DBTITLE 1,Average Visits Per Customer
sessions_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "VISITS_MEAN",
    std_column_name = "VISITS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(sessions_test[0])

# COMMAND ----------

# DBTITLE 1,Visits Per Customer
sessions_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Revenue Per Customer (RPC)
revenue_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COMBINED_REVENUE_MEAN",
    std_column_name = "COMBINED_REVENUE_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(revenue_test[0])

# COMMAND ----------

# DBTITLE 1,RPC
revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Non-Zero Revenue Per Customer (RPC)
nz_revenue_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COMBINED_REVENUE_NONZERO_MEAN",
    std_column_name = "COMBINED_REVENUE_NONZERO_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(nz_revenue_test[0])

# COMMAND ----------

# DBTITLE 1,NONZERO RPC
nz_revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average AGP Per Customer
agp_test = hypothesis_test_compare_means(
    df = agp_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "AGP_MEAN",
    std_column_name = "AGP_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(agp_test[0])

# COMMAND ----------

# DBTITLE 1,AGP Per Customer
agp_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Net Sales Per Customer
net_sales_test = hypothesis_test_compare_means(
    df = agp_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "NET_SALES_MEAN",
    std_column_name = "NET_SALES_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(net_sales_test[0])

# COMMAND ----------

# DBTITLE 1,Net Sales Per Customer
net_sales_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Margin Per Customer
try:
  margin_test = hypothesis_test_compare_means(
      df = margin_agg_df,
      variant_column_name = "VARIANT_ID",
      control_variant_name = control_variant_nm,
      mean_column_name = "MARGIN_MEAN",
      std_column_name = "MARGIN_SD",
      n_column_name = "VISITORS",
      metric_type = "mean",
      pooled = False,
      one_tailed = False,
      ci_level = 1-SIGNIFICANCE, 
      positive_good = True,
      rounding = 3
      )
    
  display(margin_test[0])
except:
  print("Margin is not computed for this test.  Please check the last time that margin was updated.")

# COMMAND ----------

# DBTITLE 1,Margin Per Customer
try:
  margin_test[1].show()
except:
  print("Margin is not computed for this test.  Please check the last time that margin was updated.")

# COMMAND ----------

# DBTITLE 1,Average TXNs Per Customer
orders_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COMBINED_ORDERS_MEAN",
    std_column_name = "COMBINED_ORDERS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(orders_test[0])

# COMMAND ----------

# DBTITLE 1,TXNs Per Customer
orders_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Units Sold Per Customer
units_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COMBINED_UNITS_MEAN",
    std_column_name = "COMBINED_UNITS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(units_test[0])

# COMMAND ----------

# DBTITLE 1,Units Per Customer
units_test[1].show()

# COMMAND ----------

# DBTITLE 1,Total Redemptions Per Customer
coupon_redemptions_test = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "REDEMPTIONS_MEAN",
    std_column_name = "REDEMPTIONS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(coupon_redemptions_test[0])

# COMMAND ----------

# DBTITLE 1,Redemptions Per Customer
coupon_redemptions_test[1].show()

# COMMAND ----------

# DBTITLE 1,Total Markdown Per Customer
mkdn_test = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "MKDN_MEAN",
    std_column_name = "MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(mkdn_test[0])

# COMMAND ----------

# DBTITLE 1,Markdown Per Customer
mkdn_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Order Value
AOV_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["COMBINED_REVENUE_MEAN","COMBINED_ORDERS_MEAN"],
    std_column_name = ["COMBINED_REVENUE_SD","COMBINED_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "COMBINED_COV_REVENUE_ORDERS",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(AOV_test[0])

# COMMAND ----------

# DBTITLE 1,AOV
AOV_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Units Per Order (Basket Size)
UPO_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["COMBINED_UNITS_MEAN","COMBINED_ORDERS_MEAN"],
    std_column_name = ["COMBINED_UNITS_SD","COMBINED_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "COMBINED_COV_UNITS_ORDERS",
    metric_type = "ratio",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(UPO_test[0])

# COMMAND ----------

# DBTITLE 1,UPO
UPO_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Engagement Metrics

# COMMAND ----------

# DBTITLE 1,Average Cart Adds Per Customer
cartadds_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "CART_ADDS_MEAN",
    std_column_name = "CART_ADDS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(cartadds_test[0])

# COMMAND ----------

# DBTITLE 1,Cart Adds Per Customer
cartadds_test[1].show()

# COMMAND ----------

# DBTITLE 1,Coupon Clips Per Customer
ACC_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COUPON_CLIPS_MEAN",
    std_column_name = "COUPON_CLIPS_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ACC_test[0])

# COMMAND ----------

# DBTITLE 1,Coupon Clips Per Customer
ACC_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Searches Per Customer
searches_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "SEARCHES_MEAN",
    std_column_name = "SEARCHES_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(searches_test[0])

# COMMAND ----------

# DBTITLE 1,Searches Per Customer
searches_test[1].show()

# COMMAND ----------

# DBTITLE 1,Add to Cart CVR
cartadds_cvr_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "UNIQUE_USERS_THAT_ADDTOCART",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(cartadds_cvr_test[0])


# COMMAND ----------

# DBTITLE 1,ATC CVR
cartadds_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,Transactions CVR
orders_cvr_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "PURCHASING_CUSTOMERS",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(orders_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,TXNs CVR
orders_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,Coupon Clips CVR
couponclips_cvr_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "UNIQUE_USERS_THAT_COUPON_CLIP",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(couponclips_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,CC CVR
couponclips_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,Search CVR
search_cvr_test = hypothesis_test_compare_means(
    df = base_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "UNIQUE_USERS_THAT_SEARCH",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(search_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,SRCH CVR
search_cvr_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Clips

# COMMAND ----------

# DBTITLE 1,Total Clips (LOY_CLIPS)
clipping_count = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "CLIPS_MEAN",
    std_column_name = "CLIPS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(clipping_count[0])

# COMMAND ----------

# DBTITLE 1,LOY CLIPS
clipping_count[1].show()

# COMMAND ----------

# DBTITLE 1,Store Coupon Clips
sc_clips = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "sc_clips_MEAN",
    std_column_name = "sc_clips_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(sc_clips[0])

# COMMAND ----------

# DBTITLE 1,STORE CLIPS
sc_clips[1].show()

# COMMAND ----------

# DBTITLE 1,Manufacturer Coupon Clips
mf_clips = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "mf_clips_MEAN",
    std_column_name = "mf_clips_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(mf_clips[0])

# COMMAND ----------

# DBTITLE 1,MANUFACTURE CLIPS
mf_clips[1].show()

# COMMAND ----------

# DBTITLE 1,Grocery Reward Clips
gr_clips = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "gr_clips_MEAN",
    std_column_name = "gr_clips_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(gr_clips[0])

# COMMAND ----------

# DBTITLE 1,GROCERY REWARD CLIPS
gr_clips[1].show()

# COMMAND ----------

# DBTITLE 1,Personalized Deals Clips
pd_clips = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "pd_clips_MEAN",
    std_column_name = "pd_clips_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(pd_clips[0])

# COMMAND ----------

# DBTITLE 1,PD CLIPS
pd_clips[1].show()

# COMMAND ----------

# DBTITLE 1,Special-Personalized Deals Clips
spd_clips = hypothesis_test_compare_means(
    df = clips_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "spd_clips_MEAN",
    std_column_name = "spd_clips_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(spd_clips[0])

# COMMAND ----------

# DBTITLE 1,SPD CLIPS
spd_clips[1].show()

# COMMAND ----------

# DBTITLE 1,PZN-Personalized Deals Clips
try:
    pzn_clips = hypothesis_test_compare_means(
        df = clips_df,
        variant_column_name = "VARIANT_ID",
        control_variant_name = control_variant_nm,
        mean_column_name = "pzn_clips_MEAN",
        std_column_name = "pzn_clips_SD",
        n_column_name = "VISITORS",
        metric_type = "mean",
        pooled = False,
        one_tailed = False,
        ci_level = 1-SIGNIFICANCE, 
        positive_good = True,
        rounding = 3
        )

    display(pzn_clips[0])
except:
    print('No PZN deals')

# COMMAND ----------

# DBTITLE 1,PZN PD CLIPS
try:
  pzn_clips[1].show()
except:
    print('No PZN deals')

# COMMAND ----------

# MAGIC %md
# MAGIC # Redemptions

# COMMAND ----------

# DBTITLE 1,Unique Redeemers 
redeeming_count = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "RPO_COMBINED_MEAN",
    std_column_name = "RPO_ECOMBINED_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(redeeming_count[0])

# COMMAND ----------

# DBTITLE 1,UNIQUE REDEEMERS
redeeming_count[1].show()

# COMMAND ----------

# DBTITLE 1,Average Total Redemptions Per Customer Ecomm
coupon_redemptions_ecomm_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_REDEMPTIONS_MEAN",
    std_column_name = "ECOMM_REDEMPTIONS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(coupon_redemptions_ecomm_test[0])

# COMMAND ----------

# DBTITLE 1,Ecomm Redemptions Per Customer
coupon_redemptions_ecomm_test[1].show()

# COMMAND ----------

# DBTITLE 1,Average Total Redemptions Per Customer In-Store
coupon_redemptions_store_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_REDEMPTIONS_MEAN",
    std_column_name = "STORE_REDEMPTIONS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(coupon_redemptions_store_test[0])

# COMMAND ----------

# DBTITLE 1,In Store Redemptions Per Customer
coupon_redemptions_store_test[1].show()

# COMMAND ----------

# DBTITLE 1,Store Coupon Redemptions
sc_redemptions = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "sc_redemptions_MEAN",
    std_column_name = "sc_redemptions_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(sc_redemptions[0])

# COMMAND ----------

# DBTITLE 1,STORE COUPON REDEMPTIONS
sc_redemptions[1].show()

# COMMAND ----------

# DBTITLE 1,Average Manufacturer Coupon Redemptions
mf_redemptions = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "mf_redemptions_MEAN",
    std_column_name = "mf_redemptions_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(mf_redemptions[0])

# COMMAND ----------

# DBTITLE 1,MANUFACTURER COUPON REDEMPTIONS
mf_redemptions[1].show()

# COMMAND ----------

# DBTITLE 1,Average Grocery Reward Redemptions
gr_redemptions = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "gr_redemptions_MEAN",
    std_column_name = "gr_redemptions_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(gr_redemptions[0])

# COMMAND ----------

# DBTITLE 1,GROCERY REWARDS REDEMPTIONS
gr_redemptions[1].show()

# COMMAND ----------

# DBTITLE 1,Average Personalized Deals Redemptions
pd_redemptions = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "pd_redemptions_MEAN",
    std_column_name = "pd_redemptions_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(pd_redemptions[0])

# COMMAND ----------

# DBTITLE 1,PD REDEMPTIONS
pd_redemptions[1].show()

# COMMAND ----------

# DBTITLE 1,Special-Personalized Deals Redemptions
spd_redemptions = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "spd_redemptions_MEAN",
    std_column_name = "spd_redemptions_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(spd_redemptions[0])

# COMMAND ----------

# DBTITLE 1,SPD Redemptions
spd_redemptions[1].show()

# COMMAND ----------

# DBTITLE 1,PZN-Personalized Deals Redemptions
try:
    pzn_redemptions = hypothesis_test_compare_means(
        df = redemptions_df,
        variant_column_name = "VARIANT_ID",
        control_variant_name = control_variant_nm,
        mean_column_name = "pzn_redemptions_MEAN",
        std_column_name = "pzn_redemptions_SD",
        n_column_name = "VISITORS",
        metric_type = "mean",
        pooled = False,
        one_tailed = False,
        ci_level = 1-SIGNIFICANCE, 
        positive_good = True,
        rounding = 3
        )

    display(pzn_redemptions[0])
except:
    print('No PZN deals')

# COMMAND ----------

# DBTITLE 1,PZN PD REDEMPTIONS
try:
  pzn_redemptions[1].show()
except:
    print('No PZN deals')

# COMMAND ----------

# MAGIC %md
# MAGIC # Categories

# COMMAND ----------

# DBTITLE 1,Categories Breadth Test
CB_test = hypothesis_test_compare_means(
    df = category_breadth_depth_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = "VARIANT A",
    mean_column_name = "CATEGORIES_MEAN",
    std_column_name = "CATEGORIES_SD",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(CB_test[0])

# COMMAND ----------

CB_test[1].show()

# COMMAND ----------

# DBTITLE 1,Category Depth Test
CD_test = hypothesis_test_compare_means(
    df = category_breadth_depth_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = "VARIANT A",
    mean_column_name = ["ITEMS_MEAN","CATEGORIES_MEAN"],
    std_column_name = ["ITEMS_SD","CATEGORIES_SD"],
    n_column_name = "UNIQUE_HOUSEHOLDS",
    cov_column_name = "CD_COV_ITEMS_CATEGORIES",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(CD_test[0])

# COMMAND ----------

CD_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Account Health

# COMMAND ----------

# DBTITLE 1,E-mail Test
acc_email_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "email_count",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_email_test[0])

# COMMAND ----------

acc_email_test[1].show()

# COMMAND ----------

# DBTITLE 1,Phone Test
acc_phone_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "phone_count",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_phone_test[0])

# COMMAND ----------

acc_phone_test[1].show()

# COMMAND ----------

# DBTITLE 1,Reachability Test
acc_reachablity_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "reachablity_score",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_reachablity_test[0])

# COMMAND ----------

acc_reachablity_test[1].show()

# COMMAND ----------

# DBTITLE 1,Name Fill Test
acc_namefill_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "fn_ln_count",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_namefill_test[0])

# COMMAND ----------

acc_namefill_test[1].show()

# COMMAND ----------

# DBTITLE 1,Birth Date Test
acc_bd_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "bday_count",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_bd_test[0])

# COMMAND ----------

acc_bd_test[1].show()

# COMMAND ----------

# DBTITLE 1,Address Fill Test
acc_address_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "address_count",
    std_column_name = "",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'rate',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(acc_address_test[0])

# COMMAND ----------

acc_address_test[1].show()

# COMMAND ----------

# DBTITLE 1,Health Score Test
acc_health_test = hypothesis_test_compare_means(
    df = account_health_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "health_score",
    std_column_name = "stdev_health_score",
    n_column_name = "UNIQUE_HOUSEHOLDS",
    metric_type = 'mean',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )
display(acc_health_test[0])

# COMMAND ----------

acc_health_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Markdown

# COMMAND ----------

# DBTITLE 1,Average Store Coupon Markdown Per Customer
sc_mkdn = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "sc_MKDN_MEAN",
    std_column_name = "sc_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(sc_mkdn[0])

# COMMAND ----------

# DBTITLE 1,STORE COUPON MARKDOWN PER CUSTOMER
sc_mkdn[1].show()

# COMMAND ----------

# DBTITLE 1,Average Manufacturer Coupon Markdown Per Customer
mf_mkdn = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "mf_MKDN_MEAN",
    std_column_name = "mf_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(mf_mkdn[0])

# COMMAND ----------

# DBTITLE 1,MANUFACTURER MARKDOWN
mf_mkdn[1].show()

# COMMAND ----------

# DBTITLE 1,Average Grocery Rewards Markdown Per Customer
gr_mkdn = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "gr_MKDN_MEAN",
    std_column_name = "gr_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(gr_mkdn[0])

# COMMAND ----------

# DBTITLE 1,GROCERY REWARDS MARKDOWN
gr_mkdn[1].show()

# COMMAND ----------

# DBTITLE 1,Average Personalized Deals Markdown Per Customer
pd_mkdn = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "pd_MKDN_MEAN",
    std_column_name = "pd_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(pd_mkdn[0])

# COMMAND ----------

# DBTITLE 1,PD DEALS MARKDOWN
pd_mkdn[1].show()

# COMMAND ----------

# DBTITLE 1,Average Special-Personalized Deals Markdown Per Customer
spd_mkdn = hypothesis_test_compare_means(
    df = redemptions_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "spd_MKDN_MEAN",
    std_column_name = "spd_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(spd_mkdn[0])

# COMMAND ----------

# DBTITLE 1,SPD DEALS MARKDOWN
spd_mkdn[1].show()

# COMMAND ----------

# DBTITLE 1,Average PZN-Personalized Deals Markdown Per Customer
try:
  pzn_mkdn = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "pzn_MKDN_MEAN",
    std_column_name = "pzn_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

  display(pzn_mkdn[0])
except:
  print('No PZN deals')

# COMMAND ----------

# DBTITLE 1,PZN PD DEALS MARKDOWN
try:
  pzn_mkdn[1].show()
except:
  print('No PZN deals')

# COMMAND ----------

# MAGIC %md
# MAGIC # Ecomm Metrics

# COMMAND ----------

# DBTITLE 1,Ecomm Revenue Per Customer
ecomm_revenue_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_REVENUE_MEAN",
    std_column_name = "ECOMM_REVENUE_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_revenue_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM RPC
ecomm_revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Non-Zero Revenue Per Customer
## ECOMM non-zero Revenue
ecomm_nonzero_revenue_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_REVENUE_NONZERO_MEAN",
    std_column_name = "ECOMM_REVENUE_NONZERO_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_nonzero_revenue_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM NONZERO RPC
ecomm_nonzero_revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Order CVR
## E-Com Order Conversion Rate
ecomm_orders_cvr_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "PURCHASING_CUSTOMERS",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_orders_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM ORDER CVR
ecomm_orders_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm TXNs Per Customer

ecomm_orders_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_ORDERS_MEAN",
    std_column_name = "ECOMM_ORDERS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_orders_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM TXNS PER CUSTOMER
ecomm_orders_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Units Per Customer
ecomm_units_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_UNITS_MEAN",
    std_column_name = "ECOMM_UNITS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_units_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM UNITS PER CUSTOMER
ecomm_units_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Average Order Value (AOV)
ecomm_AOV_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["ECOMM_REVENUE_MEAN","ECOMM_ORDERS_MEAN"],
    std_column_name = ["ECOMM_REVENUE_SD","ECOMM_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "ECOMM_COV_REVENUE_ORDERS",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_AOV_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM AOV
ecomm_AOV_test[1].show()

# COMMAND ----------

## ECOMM non-zero aov
ecomm_AOV_nonzero_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["ECOMM_REVENUE_NONZERO_MEAN","ECOMM_ORDERS_MEAN"],
    std_column_name = ["ECOMM_REVENUE_NONZERO_SD","ECOMM_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "ECOMM_COV_REVENUE_ORDERS",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_AOV_nonzero_test[0])

# COMMAND ----------

ecomm_AOV_nonzero_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Units Per Order (Basket Size)
ecomm_UPO_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["ECOMM_UNITS_MEAN","ECOMM_ORDERS_MEAN"],
    std_column_name = ["ECOMM_UNITS_SD","ECOMM_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "ECOMM_COV_UNITS_ORDERS",
    metric_type = "ratio",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_UPO_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM UPO
ecomm_UPO_test[1].show()

# COMMAND ----------

# DBTITLE 1,E-Comm Average Markdown Per Customer
ecomm_markdown_pc_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_MKDN_MEAN",
    std_column_name = "ECOMM_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_markdown_pc_test[0])

# COMMAND ----------

# DBTITLE 1,ECOMM MKDN PER CUSTOMER
ecomm_markdown_pc_test[1].show()

# COMMAND ----------

# DBTITLE 1,E-Comm BNC Test
ecomm_bnc_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_BNC_TOTAL",
    std_column_name = "ECOMM_BNC_TOTAL_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_bnc_test[0])

# COMMAND ----------

ecomm_bnc_test[1].show()

# COMMAND ----------

# DBTITLE 1,E-Comm BNC CVR Test
ecomm_bnc_cvr_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_BNC_TOTAL",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(ecomm_bnc_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,BNC CVR
ecomm_bnc_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,Ecomm Net Sales Test
ecomm_net_sales_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_NET_SALES_MEAN",
    std_column_name = "ECOMM_NET_SALES_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_net_sales_test[0])

# COMMAND ----------

ecomm_net_sales_test[1]

# COMMAND ----------

# DBTITLE 1,Ecomm Non Zero Net Sales Test
ecomm_nz_net_sales_test = hypothesis_test_compare_means(
    df = ecomm_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "ECOMM_NET_SALES_NONZERO_MEAN",
    std_column_name = "ECOMM_NET_SALES_NONZERO_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(ecomm_nz_net_sales_test[0])

# COMMAND ----------

ecomm_nz_net_sales_test[1]

# COMMAND ----------

# MAGIC %md
# MAGIC # In-Store Metrics

# COMMAND ----------

# DBTITLE 1,In-Store Revenue Per Customer
store_revenue_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_REVENUE_MEAN",
    std_column_name = "STORE_REVENUE_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(store_revenue_test[0])

# COMMAND ----------

# DBTITLE 1,In-Store RPC
store_revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store NonZero Revenue Per Customer
## Store non-zero revenue
store_revenue_nonzero_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_REVENUE_NONZERO_MEAN",
    std_column_name = "STORE_REVENUE_NONZERO_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(store_revenue_nonzero_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE NONZERO RPC
store_revenue_nonzero_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store Order CVR
## In-Store Order Conversion Rate
store_orders_cvr_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "PURCHASING_CUSTOMERS",
    std_column_name = "",
    n_column_name = "VISITORS",
    metric_type = 'rate',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(store_orders_cvr_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE ORDER CVR
store_orders_cvr_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store TXNs Per Customer
store_orders_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_ORDERS_MEAN",
    std_column_name = "STORE_ORDERS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(store_orders_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE TXNs PER CUSTOMER
store_orders_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store Average Units Sold Per Customer
store_units_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_UNITS_MEAN",
    std_column_name = "STORE_UNITS_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(store_units_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE UNITS PER CUSTOMER
store_units_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store Average Order Value (AOV)
store_AOV_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["STORE_REVENUE_MEAN","STORE_ORDERS_MEAN"],
    std_column_name = ["STORE_REVENUE_SD","STORE_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "STORE_COV_REVENUE_ORDERS",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(store_AOV_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE AOV
store_AOV_test[1].show()

# COMMAND ----------

## Store non-zero aov

store_AOV_nonzero_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["STORE_REVENUE_NONZERO_MEAN","STORE_ORDERS_MEAN"],
    std_column_name = ["STORE_REVENUE_NONZERO_SD","STORE_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "STORE_COV_REVENUE_ORDERS",
    metric_type = 'ratio',
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(store_AOV_nonzero_test[0])

# COMMAND ----------

store_AOV_nonzero_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store Average Units Per Order (Basket Size)
store_UPO_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = ["STORE_UNITS_MEAN","STORE_ORDERS_MEAN"],
    std_column_name = ["STORE_UNITS_SD","STORE_ORDERS_SD"],
    n_column_name = "VISITORS",
    cov_column_name = "STORE_COV_UNITS_ORDERS",
    metric_type = "ratio",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(store_UPO_test[0])

# COMMAND ----------

# DBTITLE 1,IN-STORE UPO
store_UPO_test[1].show()

# COMMAND ----------

# DBTITLE 1,In-Store Average Markdown Per Customer
store_markdown_pc_test = hypothesis_test_compare_means(
    df = store_agg_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "STORE_MKDN_MEAN",
    std_column_name = "STORE_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = 'mean',
    pooled = True,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 4
    )

display(store_markdown_pc_test[0])

# COMMAND ----------

# DBTITLE 1,INSTORE MKDN PER CUSTOMER
store_markdown_pc_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Basket Health

# COMMAND ----------

# DBTITLE 1,Basket Health Mean Rate
bh_test = hypothesis_test_compare_means(
    df = basket_health_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "BASKET_RATE_MEAN",
    std_column_name = "BASKET_RATE_SD",
    n_column_name = "UNIQUE_BASKET_HEALTH_HH",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(bh_test[0])

# COMMAND ----------

# DBTITLE 1,Basket Health Mean Rate
bh_test[1].show()

# COMMAND ----------

# DBTITLE 1,Basket Health (AB) Mean Rate
bh_ab_test = hypothesis_test_compare_means(
    df = basket_health_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "AB_RATE_MEAN",
    std_column_name = "AB_RATE_SD",
    n_column_name = "UNIQUE_BASKET_HEALTH_HH",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(bh_ab_test[0])

# COMMAND ----------

# DBTITLE 1,Basket Health (AB) Mean Rate
bh_ab_test[1].show()

# COMMAND ----------

# DBTITLE 1,Basket Health (DE) Mean Rate
bh_de_test = hypothesis_test_compare_means(
    df = basket_health_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "DE_RATE_MEAN",
    std_column_name = "DE_RATE_SD",
    n_column_name = "UNIQUE_BASKET_HEALTH_HH",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(bh_de_test[0])

# COMMAND ----------

# DBTITLE 1,Basket Health (DE) Mean Rate
bh_de_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # SNAP Metrics

# COMMAND ----------

# DBTITLE 1,Combined SNAP Tender Per Customer
snap_test = hypothesis_test_compare_means(
    df = combined_df,
    variant_column_name = "VARIANT_ID",
    control_variant_name = control_variant_nm,
    mean_column_name = "COMBINED_SNAP_MEAN",
    std_column_name = "COMBINED_SNAP_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(snap_test[0])

# COMMAND ----------

# DBTITLE 1,SNAP PER CUSTOMER
snap_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gas Metrics

# COMMAND ----------

# DBTITLE 1,Gas Revenue Per Customer
gas_revenue_test = hypothesis_test_compare_means(
    df = gas_agg_df,
    variant_column_name = "VARIANT_ID_GAS",
    control_variant_name = control_variant_nm,
    mean_column_name = "GAS_REVENUE_MEAN",
    std_column_name = "GAS_REVENUE_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(gas_revenue_test[0])

# COMMAND ----------

# DBTITLE 1,GAS REVENUE PER CUSTOMER
gas_revenue_test[1].show()

# COMMAND ----------

# DBTITLE 1,Gas Markdown
gas_mkdn_test = hypothesis_test_compare_means(
    df = gas_agg_df,
    variant_column_name = "VARIANT_ID_GAS",
    control_variant_name = control_variant_nm,
    mean_column_name = "GAS_MKDN_MEAN",
    std_column_name = "GAS_MKDN_SD",
    n_column_name = "VISITORS",
    metric_type = "mean",
    pooled = False,
    one_tailed = False,
    ci_level = 1-SIGNIFICANCE, 
    positive_good = True,
    rounding = 3
    )

display(gas_mkdn_test[0])

# COMMAND ----------

gas_mkdn_test[1].show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EMAIL/PUSH/SMS HYPOTHESIS TEST 

# COMMAND ----------

# email_optin_test = hypothesis_test_compare_means(
#     df = combined_df,
#     variant_column_name = "VARIANT_ID",
#     control_variant_name = control_variant_nm,
#     mean_column_name = "EMAIL_OPTIN_COUNT",
#     std_column_name = "",
#     n_column_name = "VISITORS",
#     metric_type = 'rate',
#     pooled = True,
#     one_tailed = False,
#     ci_level = 1-SIGNIFICANCE, 
#     positive_good = True,
#     rounding = 4
#     )

# display(email_optin_test[0])

# COMMAND ----------

# email_optin_test = hypothesis_test_compare_means(
#     df = combined_df,
#     variant_column_name = "VARIANT_ID",
#     control_variant_name = control_variant_nm,
#     mean_column_name = "SMS_OPTIN_COUNT",
#     std_column_name = "",
#     n_column_name = "VISITORS",
#     metric_type = 'rate',
#     pooled = True,
#     one_tailed = False,
#     ci_level = 1-SIGNIFICANCE, 
#     positive_good = True,
#     rounding = 4
#     )

# display(email_optin_test[0])

# COMMAND ----------

# push_enabled_test = hypothesis_test_compare_means(
#     df = combined_df,
#     variant_column_name = "VARIANT_ID",
#     control_variant_name = control_variant_nm,
#     mean_column_name = "PUSH_ENABLED_COUNT",
#     std_column_name = "",
#     n_column_name = "VISITORS",
#     metric_type = 'rate',
#     pooled = True,
#     one_tailed = False,
#     ci_level = 1-SIGNIFICANCE, 
#     positive_good = True,
#     rounding = 4
#     )

# display(push_enabled_test[0])

# COMMAND ----------

# DBTITLE 1,Results Summary
# Function to process test results into a structured format
def process_ttest_result(df, metric_name):
   if df is None or df.empty:
       print(f"Skipping {metric_name} due to missing data")
       return None
   df["Variant"] = df["Variant"].str.upper()  # Standardize variant names
   control_row = df[df["Variant"] == "CONTROL"]
   if control_row.empty:
       print(f"Skipping {metric_name} due to missing CONTROL group")
       return None
   variant_rows = df[df["Variant"] != "CONTROL"]  # Get all other variants
   structured_data = {"Variant": ["CONTROL"], metric_name: [control_row["Mean"].values[0]]}
   # Append all variant means first
   for _, variant_row in variant_rows.iterrows():
       structured_data["Variant"].append(variant_row["Variant"])
       structured_data[metric_name].append(variant_row["Mean"])
   # Append Absolute Delta, Relative Delta, and P-Values
   for metric_type in ["Absolute Delta", "Relative Delta", "P-Value"]:
    #    structured_data["Variant"].append(metric_type)  # Single row label for each section
    #    structured_data[metric_name].append("")  # Placeholder for format consistency
       for _, variant_row in variant_rows.iterrows():
           structured_data["Variant"].append(f"{variant_row['Variant']} {metric_type}")
           structured_data[metric_name].append(variant_row[metric_type])
   return pd.DataFrame(structured_data)
# List of t-test dataframes to process

ttest_results = [
    ("Customers", visitors_test[0]),
   ("Total Revenue Per Customer", revenue_test[0]),
   ("Non Zero Revenue RPC", nz_revenue_test[0]),
   ("Net Sales Per Customer", net_sales_test[0]),
   ("Ecomm Revenue per Customer", ecomm_revenue_test[0]),
   ("Ecomm Non Zero RPC", ecomm_nonzero_revenue_test[0]),
   ("Markdown Per Customer", mkdn_test[0]),
   ("AGP Per Customer", agp_test[0]),
   ("CVR", orders_cvr_test[0]),
   ("AOV", AOV_test[0]),
   ("UPO", UPO_test[0]),
   ("Transactions Per Customer", orders_test[0]),
   ("Units Per Customer", units_test[0]),
   ("Category Breadth", CB_test[0]),
   ("Category Depth", CD_test[0]),
   ("Visits Per Customer", sessions_test[0]),
   ("Searches Per Customer", searches_test[0]),
   ("Cart Adds", cartadds_test[0]),
   ("Coupon Clips Per Customer", ACC_test[0]),
   ("Redemptions Per Customer", coupon_redemptions_test[0]),
   ("% of Customers who Search", search_cvr_test[0]),
   ("% of Customers who Add to Cart", cartadds_cvr_test[0]),
   ("% of Customers who Clip", couponclips_cvr_test[0]),
   ("Redemptions Per Customer - Ecomm", coupon_redemptions_ecomm_test[0]),
    ("Ecomm Net Sales",ecomm_net_sales_test[0]),
   ("Ecomm Non Zero Net Sales",ecomm_nz_net_sales_test[0]),
   ("Ecomm MKDN per Customer", ecomm_markdown_pc_test[0]),
   ("Ecomm CVR", ecomm_orders_cvr_test[0]),
   ("Ecomm TXNs Per Customer", ecomm_orders_test[0]),
   ("Ecomm BNC",ecomm_bnc_test[0]),
   ("Ecomm BNC CVR", ecomm_bnc_cvr_test[0])
]

# Process each test result and merge them into a summary DataFrame
summary_dfs = [process_ttest_result(df, name) for name, df in ttest_results if process_ttest_result(df, name) is not None]
# Extract "Variant" column from the first DataFrame (avoid duplication)
variant_column = summary_dfs[0][["Variant"]].copy()
# Drop "Variant" from all other DataFrames before merging
for i in range(len(summary_dfs)):
   summary_dfs[i] = summary_dfs[i].drop(columns=["Variant"], errors="ignore")
# Merge all processed DataFrames on their row index
summary_df = pd.concat([variant_column] + summary_dfs, axis=1)
# Ensure "Variant" is in Databricks display
summary_df.reset_index(drop=True, inplace=True)
# Display the structured DataFrame in Databricks
display(summary_df) 

# COMMAND ----------

# DBTITLE 1,Numeriic Results Summary
import pandas as pd
import numpy as np
# Function to process test results into structured format
def process_ttest_result(df, metric_name):
   if df is None or df.empty:
       print(f"Skipping {metric_name} due to missing data")
       return None
   # Standardize variant names
   df["Variant"] = df["Variant"].str.upper()
   # Define numeric columns (including "Relative Delta")
   numeric_cols = ['Mean', 'Absolute Delta', 'Relative Delta', 'P-Value']
   # Remove '%' sign before converting to float
   for col in numeric_cols:
       if col in df.columns:
           df[col] = df[col].astype(str).str.replace('%', '', regex=True)  # Remove %
           df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float
   # Validate control group presence
   control_row = df[df["Variant"] == "CONTROL"]
   if control_row.empty:
       print(f"Skipping {metric_name} due to missing CONTROL group")
       return None
   # Separate control and variants
   variant_rows = df[df["Variant"] != "CONTROL"]
   # Structured data storage
   structured_data = {
       "Variant": ["CONTROL"],
       metric_name: [control_row["Mean"].values[0]]
   }
   # Add variant means
   for _, variant_row in variant_rows.iterrows():
       structured_data["Variant"].append(variant_row["Variant"])
       structured_data[metric_name].append(variant_row["Mean"])
   # Add metric sections with NaN placeholders
   for metric_type in ["Absolute Delta", "Relative Delta", "P-Value"]:
    #    structured_data["Variant"].append(metric_type)  # Section header
    #    structured_data[metric_name].append(np.nan)  # NaN placeholder
       # Add variant-specific values
       for _, variant_row in variant_rows.iterrows():
           structured_data["Variant"].append(f"{variant_row['Variant']} {metric_type}")
           structured_data[metric_name].append(variant_row.get(metric_type, np.nan))
   return pd.DataFrame(structured_data)
# Your original list of t-test results (preserved exactly)
ttest_results = [
    ("Customers", visitors_test[0]),
   ("Total Revenue Per Customer", revenue_test[0]),
   ("Non Zero Revenue RPC", nz_revenue_test[0]),
   ("Net Sales Per Customer", net_sales_test[0]),
   ("Ecomm Revenue per Customer", ecomm_revenue_test[0]),
   ("Ecomm Non Zero RPC", ecomm_nonzero_revenue_test[0]),
   ("Markdown Per Customer", mkdn_test[0]),
   ("AGP Per Customer", agp_test[0]),
   ("CVR", orders_cvr_test[0]),
   ("AOV", AOV_test[0]),
   ("UPO", UPO_test[0]),
   ("Transactions Per Customer", orders_test[0]),
   ("Units Per Customer", units_test[0]),
   ("Category Breadth", CB_test[0]),
   ("Category Depth", CD_test[0]),
   ("Visits Per Customer", sessions_test[0]),
   ("Searches Per Customer", searches_test[0]),
   ("Cart Adds", cartadds_test[0]),
   ("Coupon Clips Per Customer", ACC_test[0]),
   ("Redemptions Per Customer", coupon_redemptions_test[0]),
   ("% of Customers who Search", search_cvr_test[0]),
   ("% of Customers who Add to Cart", cartadds_cvr_test[0]),
   ("% of Customers who Clip", couponclips_cvr_test[0]),
   ("Redemptions Per Customer - Ecomm", coupon_redemptions_ecomm_test[0]),
   ("Ecomm MKDN per Customer", ecomm_markdown_pc_test[0]),
   ("Ecomm CVR", ecomm_orders_cvr_test[0]),
   ("Ecomm TXNs Per Customer", ecomm_orders_test[0]),
   ("Ecomm Net Sales",ecomm_net_sales_test[0]),
   ("Ecomm Non Zero Net Sales",ecomm_nz_net_sales_test[0]),
   ("Ecomm BNC",ecomm_bnc_test[0]),
   ("Ecomm BNC CVR", ecomm_bnc_cvr_test[0]),
]

# Process all results
summary_dfs = []
for name, df in ttest_results:
   processed_df = process_ttest_result(df, name)
   if processed_df is not None:
       summary_dfs.append(processed_df)
# Merge results
if summary_dfs:
   variant_column = summary_dfs[0][["Variant"]].copy()
   # Drop variant column from other DFs before merging
   for df in summary_dfs:
       df.drop(columns=["Variant"], errors="ignore", inplace=True)
   # Concatenate all data
   summary_df = pd.concat([variant_column] + summary_dfs, axis=1, join="outer")
   # Convert numeric columns
   for col in summary_df.columns.difference(["Variant"]):
       summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")
   # Reset index for clean display
   summary_df.reset_index(drop=True, inplace=True)
   # Debugging Check: Verify if "Relative Delta" and "CVR" are correctly processed
   print("✅ Final DataFrame ready for display in Databricks.")
   # Display the final DataFrame (Databricks method)
   display(summary_df)
else:
   print("No valid data to display")

# COMMAND ----------

# DBTITLE 1,Formatting
def process_ttest_result(df, metric_name):
   if df is None or df.empty:
       print(f"Skipping {metric_name} due to missing data")
       return None
   df["Variant"] = df["Variant"].str.upper()
   numeric_cols = ['Mean', 'Absolute Delta', 'Relative Delta', 'P-Value']
   for col in numeric_cols:
       if col in df.columns:
           df[col] = df[col].astype(str).str.replace('%', '', regex=True)
           df[col] = pd.to_numeric(df[col], errors='coerce')
   control_row = df[df["Variant"] == "CONTROL"]
   if control_row.empty:
       print(f"Skipping {metric_name} due to missing CONTROL group")
       return None
   variant_rows = df[df["Variant"] != "CONTROL"]
   structured_data = {
       "Variant": ["CONTROL"],
       metric_name: [control_row["Mean"].values[0]]
   }
   for _, variant_row in variant_rows.iterrows():
       structured_data["Variant"].append(variant_row["Variant"])
       structured_data[metric_name].append(variant_row["Mean"])
   for metric_type in ["Absolute Delta", "Relative Delta", "P-Value"]:
    #    structured_data["Variant"].append(metric_type)
    #    structured_data[metric_name].append(np.nan)
       for _, variant_row in variant_rows.iterrows():
           structured_data["Variant"].append(f"{variant_row['Variant']} {metric_type}")
           structured_data[metric_name].append(variant_row.get(metric_type, np.nan))
   return pd.DataFrame(structured_data)
ttest_results = [
    ("Customers", visitors_test[0]),
   ("Total Revenue Per Customer", revenue_test[0]),
   ("Non Zero Revenue RPC", nz_revenue_test[0]),
   ("Net Sales Per Customer", net_sales_test[0]),
   ("Ecomm Revenue per Customer", ecomm_revenue_test[0]),
   ("Ecomm Non Zero RPC", ecomm_nonzero_revenue_test[0]),
   ("Markdown Per Customer", mkdn_test[0]),
   ("AGP Per Customer", agp_test[0]),
   ("CVR", orders_cvr_test[0]),
   ("AOV", AOV_test[0]),
   ("UPO", UPO_test[0]),
   ("Transactions Per Customer", orders_test[0]),
   ("Units Per Customer", units_test[0]),
   ("Category Breadth", CB_test[0]),
   ("Category Depth", CD_test[0]),
   ("Visits Per Customer", sessions_test[0]),
   ("Searches Per Customer", searches_test[0]),
   ("Cart Adds", cartadds_test[0]),
   ("Coupon Clips Per Customer", ACC_test[0]),
   ("Redemptions Per Customer", coupon_redemptions_test[0]),
   ("% of Customers who Search", search_cvr_test[0]),
   ("% of Customers who Add to Cart", cartadds_cvr_test[0]),
   ("% of Customers who Clip", couponclips_cvr_test[0]),
   ("Redemptions Per Customer - Ecomm", coupon_redemptions_ecomm_test[0]),
   ("Ecomm MKDN per Customer", ecomm_markdown_pc_test[0]),
   ("Ecomm CVR", ecomm_orders_cvr_test[0]),
   ("Ecomm TXNs Per Customer", ecomm_orders_test[0]),
   ("Ecomm Net Sales",ecomm_net_sales_test[0]),
   ("Ecomm Non Zero Net Sales",ecomm_nz_net_sales_test[0]),
   ("Ecomm BNC",ecomm_bnc_test[0]),
   ("Ecomm BNC CVR", ecomm_bnc_cvr_test[0]),
]

summary_dfs = []
for name, df in ttest_results:
   processed_df = process_ttest_result(df, name)
   if processed_df is not None:
       summary_dfs.append(processed_df)
if summary_dfs:
   variant_column = summary_dfs[0][["Variant"]].copy()
   for df in summary_dfs:
       df.drop(columns=["Variant"], errors="ignore", inplace=True)
   summary_df = pd.concat([variant_column] + summary_dfs, axis=1, join="outer")
   for col in summary_df.columns.difference(["Variant"]):
       summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")
   summary_df.reset_index(drop=True, inplace=True)
   # Define formatting rules using lists
   formatting = {
       'no_decimals': ["Customers", "Visits", "Ecomm BNC"],
       'two_decimals': [
           "Units Per Customer", "Searches Per Customer", "Cart Adds",
           "Coupon Clips Per Customer", "Redemptions Per Customer",
           "Redemptions Per Customer - Ecomm","UPO" ,"Ecomm TXNs Per Customer","Transactions Per Customer",
           "Category Breadth","Category Depth"
       ],
       'dollars': [
           "Total Revenue Per Customer", "Non Zero Revenue RPC", "Net Sales Per Customer",
           "Ecomm Revenue per Customer", "Markdown Per Customer","Ecomm Non Zero RPC",
           "AGP Per Customer", "AOV", "Ecomm MKDN per Customer", "Ecomm Net Sales", "Ecomm Non Zero Net Sales"
       ],
       'percentages': [
           "CVR", "% of Customers who Search", "% of Customers who Add to Cart",
           "% of Customers who Clip", "Ecomm CVR", "Ecomm BNC CVR"
       ]
   }

   # Create a formatted copy of the DataFrame
   formatted_df = summary_df.copy()
   # Identify special rows (needs to be done before any formatting)
   p_value_mask = formatted_df['Variant'].str.contains('P-Value', na=False)
   relative_delta_mask = formatted_df['Variant'].str.contains('Relative Delta', na=False)
   special_mask = p_value_mask | relative_delta_mask
   
   # Apply column formatting to ALL NON-special rows
   for category, cols in formatting.items():
       for col in cols:
           if col not in formatted_df.columns:
               continue
           non_special_mask = ~special_mask
           if category == 'no_decimals':
               formatted_df.loc[non_special_mask, col] = formatted_df.loc[non_special_mask, col].apply(
                   lambda x: f"{float(x):,.0f}" if pd.notnull(x) else "")
           elif category == 'two_decimals': 
               formatted_df.loc[non_special_mask, col] = formatted_df.loc[non_special_mask, col].apply(
               lambda x: f"{float(x):,.2f}" if pd.notnull(x) else "")
           elif category == 'dollars':
               formatted_df.loc[non_special_mask, col] = formatted_df.loc[non_special_mask, col].apply(
                   lambda x: f"${float(x):,.2f}" if pd.notnull(x) else "")
           elif category == 'percentages':
               formatted_df.loc[non_special_mask, col] = formatted_df.loc[non_special_mask, col].apply(
               lambda x: f"{float(x):.2f}%" if pd.notnull(x) else "")
               
   # Format p-value rows with 3 decimal places
   for col in formatted_df.columns.difference(['Variant']):
       formatted_df.loc[p_value_mask, col] = (
           formatted_df.loc[p_value_mask, col]
           .apply(pd.to_numeric, errors='coerce')
           .apply(lambda x: f"{x:.3f}" if pd.notnull(x) else ""))


   # Format for relative delta
   for col in formatted_df.columns.difference(['Variant']):
       formatted_df.loc[relative_delta_mask, col] = (
           formatted_df.loc[relative_delta_mask, col]
           .astype(str)
           .str.replace('%', '', regex=False)
           .apply(pd.to_numeric, errors='coerce')
           .apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else ""))
       
   print("✅ Final DataFrame formatted with proper numeric displays.")
   display(formatted_df)
else:
   print("No valid data to display")

# COMMAND ----------

# DBTITLE 1,Scroll Table
# def style_summary_table(formatted_df):
#    """Generate 1920px-fixed table with restored header colors"""
#    df = formatted_df.copy().fillna('')
#    # ======================================================================
#    # COLUMN CATEGORIES (MODIFY THESE TO MATCH YOUR DATA)
#    # ======================================================================
#    transaction_columns = [
#        "Customers","RPC Test", "Non Zero Revenue Test","Net Sales Per Customer", 
#        "AGP Test",
#        "CVR", "AOV Test", "UPO Test", "Transactions Per Customer",
#        "Units Per Customer","Ecomm Revenue per Customer Test","Markdown Per Customer",
#        "Revenue Per Customer", "Non Zero Revenue RPC", "Ecomm Revenue per Customer",
#        "Markdown Per Customer", "AGP", "Transactions CVR", "AOV", "UPO"
#    ]
#    engagement_columns = [
#        "Visits", "Searches Per Customer", "Cart Adds",
#        "Coupon Clips Per Customer", "Redemptions Per Customer",
#        "Search CVR", "Cart Adds CVR", "Coupons Clips CVR","Redemptions Per Customer - Ecomm"
#    ]
#    ecomm_columns = [
#        "Ecomm Revenue Per Customer Test", "Ecomm MKDN per Customer",
#        "Ecomm CVR", "Ecomm TXNs Per Customer", "Ecomm BNC CVR"
#    ]
#    exclude_from_coloring = ["Customers"]
#    # ======================================================================
#    # UPDATED CSS WITH HEADER COLOR FIXES
#    # ======================================================================
#    styled_html = f"""
# <style>
#    .dashboard-container {{
#        max-width: 1920px;
#        margin: 0 auto;
#        font-family: Arial, sans-serif;
#    }}
#    .compact-table {{
#        width: 100%;
#        border-collapse: collapse;
#        table-layout: fixed;
#        font-size: 12px;
#    }}
#    .compact-table th {{
#        color: #2c3e50;
#        padding: 6px 8px;
#        border: 1px solid #dee2e6;
#        font-weight: bold;
#        overflow: hidden;
#    }}
#    /* Header Color Fixes */
#    .transaction-header {{
#        background-color: #d0e0f5 !important;
#        min-width: 120px;
#    }}
#    .engagement-header {{
#        background-color: #f7d7af !important;
#        min-width: 110px;
#    }}
#    .ecomm-header {{
#        background-color: #e2e3e5 !important;
#        min-width: 130px;
#    }}
#    .variant-header {{
#        background-color: #bfbfbf !important;
#        width: 150px;
#    }}
#    .compact-table td {{
#        padding: 6px 8px;
#        border: 1px solid #dee2e6;
#        text-align: right;
#        vertical-align: middle;
#        white-space: nowrap;
#    }}
#    /* Conditional Formatting */
#    .positive {{ background-color: #C6EFCE !important; color: #006100 !important; }}
#    .negative {{ background-color: #FFC7CE !important; color: #9C0006 !important; }}
#    .significant {{ border: 2px solid #5B9BD5 !important; }}
#    /* Legend Styling */
#    .compact-legend {{
#        margin: 15px 0;
#        padding: 12px;
#        background: #f8f9fa;
#        border-radius: 6px;
#        display: flex;
#        gap: 25px;
#        justify-content: center;
#    }}
#    .legend-item {{
#        display: flex;
#        align-items: center;
#        gap: 6px;
#        font-size: 12px;
#    }}
#    .legend-color {{
#        width: 16px;
#        height: 16px;
#        border-radius: 3px;
#    }}
# </style>
# <div class="dashboard-container">
# <table class="compact-table">
# <colgroup>
#            {' '.join([f'<col style="width: 150px">' if col == "Variant" else
#                      f'<col style="width: 120px">' if col in transaction_columns else
#                      f'<col style="width: 110px">' if col in engagement_columns else
#                      f'<col style="width: 130px">' for col in df.columns])}
# </colgroup>
# <thead>
# <tr>
#                {' '.join([f'<th class="variant-header">{col}</th>' if col == "Variant" else
#                          f'<th class="transaction-header">{col}</th>' if col in transaction_columns else
#                          f'<th class="engagement-header">{col}</th>' if col in engagement_columns else
#                          f'<th class="ecomm-header">{col}</th>' for col in df.columns])}
# </tr>
# </thead>
# <tbody>"""
#    # ======================================================================
#    # CONDITIONAL FORMATTING LOGIC (PRESERVES FORMATTED VALUES)
#    # ======================================================================
#    pvalue_lookup = {}
#    for idx, row in df.iterrows():
#        if "P-Value" in row["Variant"]:
#            variant = row["Variant"].replace(" P-Value", "")
#            for metric in df.columns[1:]:
#                raw_value = str(row[metric]).replace('$','').replace('%','').replace(',','').strip()
#                try:
#                    pvalue_lookup[(variant, metric)] = float(raw_value) if raw_value else 1.0
#                except:
#                    pvalue_lookup[(variant, metric)] = 1.0
#    for idx, row in df.iterrows():
#        styled_html += "<tr>"
#        for col in df.columns:
#            cell_value = row[col]
#            cell_class = ""
#            if col == "Variant":
#                styled_html += f'<td>{cell_value}</td>'
#                continue
#            if "Relative Delta" in row["Variant"]:
#                try:
#                    variant = row["Variant"].replace(" Relative Delta", "")
#                    pvalue = pvalue_lookup.get((variant, col), 1)
#                    if col not in exclude_from_coloring:
#                        clean_value = str(cell_value).replace('$','').replace('%','').replace(',','').strip()
#                        numeric_value = float(clean_value) if clean_value else 0
#                        if pvalue < 0.05:
#                            cell_class = "positive" if numeric_value > 0 else "negative"
#                except: pass
#            if "P-Value" in row["Variant"]:
#                try:
#                    if col not in exclude_from_coloring:
#                        clean_value = str(cell_value).replace('$','').replace('%','').replace(',','').strip()
#                        if float(clean_value) < 0.05:
#                            cell_class = "significant"
#                except: pass
#            styled_html += f'<td class="{cell_class}">{cell_value}</td>'
#        styled_html += "</tr>"
#    # ======================================================================
#    # COMPACT LEGEND
#    # ======================================================================
#    styled_html += f"""
# </tbody>
# </table>
# <div class="compact-legend">
# <div class="legend-item">
# <div class="legend-color" style="background-color: #d0e0f5"></div>
# <span>Transaction Metrics</span>
# </div>
# <div class="legend-item">
# <div class="legend-color" style="background-color: #f7d7af"></div>
# <span>Engagement Metrics</span>
# </div>
# <div class="legend-item">
# <div class="legend-color" style="background-color: #e2e3e5"></div>
# <span>Ecomm Metrics</span>
# </div>
# </div>
# </div>"""
#    return styled_html
# # Usage formatted_df
# displayHTML(style_summary_table(formatted_df))

# COMMAND ----------

# DBTITLE 1,Break Out Table
def style_summary_table(formatted_df):
   """Generate three 1920px-fixed tables with improved border visibility"""
   df = formatted_df.copy().fillna('')
   # ======================================================================
   # COLUMN CATEGORIES
   # ======================================================================
   transaction_columns = [
       "Customers","Total Revenue Per Customer", "Non Zero Revenue RPC", "Net Sales Per Customer","Markdown Per Customer",
       "AGP Per Customer","CVR", "AOV", "UPO", "Transactions Per Customer", "Units Per Customer","Category Breadth","Category Depth"
   ]
   engagement_columns = [
       "Visits Per Customer", "Searches Per Customer", "Cart Adds",
       "Coupon Clips Per Customer", "Redemptions Per Customer",
       "% of Customers who Search", "% of Customers who Add to Cart", "% of Customers who Clip"
   ]
   ecomm_columns = [
       "Ecomm Revenue per Customer","Ecomm Non Zero RPC","Ecomm Net Sales", "Ecomm Non Zero Net Sales",
       "Ecomm MKDN per Customer", "Redemptions Per Customer - Ecomm",
       "Ecomm CVR", "Ecomm TXNs Per Customer", "Ecomm BNC", "Ecomm BNC CVR",
   ]
   exclude_from_coloring = ["Customers","Ecomm BNC"]

   reverse_color_columns = [
       "Markdown Per Customer",
       "Ecomm MKDN per Customer"
   ]
   def validate_columns(expected_cols, category_name):
       missing = [col for col in expected_cols if col not in df.columns]
       if missing:
           print(f"⚠️ Warning - {category_name} columns missing: {missing}")
   validate_columns(["Variant"] + transaction_columns, "Transaction")
   validate_columns(["Variant"] + engagement_columns, "Engagement")
   validate_columns(["Variant"] + ecomm_columns, "Ecomm")
   # ======================================================================
   # P-VALUE LOOKUP
   # ======================================================================
   pvalue_lookup = {}
   for idx, row in df.iterrows():
       if "P-Value" in row["Variant"]:
           variant = row["Variant"].replace(" P-Value", "")
           for metric in df.columns[1:]:
               raw_value = str(row[metric]).replace('$','').replace('%','').replace(',','').strip()
               try:
                   pvalue_lookup[(variant, metric)] = float(raw_value) if raw_value else 1.0
               except:
                   pvalue_lookup[(variant, metric)] = 1.0
   # ======================================================================
   # HTML GENERATION WITH PADDING
   # ======================================================================
   category_configs = [
       {
           "name": "Transaction",
           "columns": transaction_columns,
           "header_class": "transaction-header",
           "legend_color": "#d0e0f5",
           "col_width": 120
       },
       {
           "name": "Engagement",
           "columns": engagement_columns,
           "header_class": "engagement-header",
           "legend_color": "#f7d7af",
           "col_width": 110
       },
       {
           "name": "Ecomm",
           "columns": ecomm_columns,
           "header_class": "ecomm-header",
           "legend_color": "#e2e3e5",
           "col_width": 130
       }
   ]
   html_outputs = []
   for config in category_configs:
       subset_cols = ["Variant"] + [col for col in config["columns"] if col in df.columns]
       if len(subset_cols) == 1: continue
       df_subset = df[subset_cols]
       styled_html = f"""
<style>
   .dashboard-container {{
       max-width: 1920px;
       margin: 0 auto;
       padding: 0 20px;
       font-family: Arial, sans-serif;
       box-sizing: border-box;
   }}
   .compact-table {{
       width: 100%;
       border-collapse: collapse;
       table-layout: fixed;
       font-size: 12px;
       margin: 15px 0;
       border: 1px solid #dee2e6;
   }}
   .compact-table th {{
       color: #2c3e50;
       padding: 8px 12px;
       border: 1px solid #dee2e6;
       font-weight: bold;
       background-clip: padding-box;
   }}
   .transaction-header {{ background-color: #d0e0f5 !important; }}
   .engagement-header {{ background-color: #f7d7af !important; }}
   .ecomm-header {{ background-color: #e2e3e5 !important; }}
   .variant-header {{ background-color: #bfbfbf !important; }}
   .compact-table td {{
       padding: 8px 12px;
       border: 1px solid #dee2e6;
       text-align: right;
       vertical-align: middle;
       white-space: nowrap;
       position: relative;
   }}
   .positive {{ background-color: #C6EFCE !important; color: #006100 !important; }}
   .negative {{ background-color: #FFC7CE !important; color: #9C0006 !important; }}
   .absdelta {{ background-color: #f7f7f7 !important; }}
   .significant {{ border: 2px solid #5B9BD5 !important; }}
   .legend-container {{
       margin: 20px 0;
       padding: 15px;
       background: #f8f9fa;
       border-radius: 8px;
       display: flex;
       justify-content: center;
       gap: 25px;
   }}
   .legend-item {{
       display: flex;
       align-items: center;
       gap: 8px;
       font-size: 13px;
   }}
   .legend-color {{
       width: 18px;
       height: 18px;
       border-radius: 4px;
       border: 1px solid #dee2e6;
   }}
</style>
<div class="dashboard-container">
<h3 style="margin-bottom: 10px;">{config['name']} Metrics</h3>
<table class="compact-table">
<colgroup>
       {' '.join([f'<col style="width: 150px">' if col == "Variant"
                else f'<col style="width: {config["col_width"]}px">'
                for col in df_subset.columns])}
</colgroup>
<thead>
<tr>
           {' '.join([f'<th class="variant-header">{col}</th>' if col == "Variant"
                    else f'<th class="{config["header_class"]}">{col}</th>'
                    for col in df_subset.columns])}
</tr>
</thead>
<tbody>"""
       for idx, row in df_subset.iterrows():
           styled_html += "<tr>"
           for col in df_subset.columns:
               cell_value = row[col]
               cell_class = ""
               if col == "Variant":
                   styled_html += f'<td>{cell_value}</td>'
                   continue
               if str(cell_value).strip() == col:
                   cell_value = ''
               if "Absolute Delta" in row["Variant"]:
                   cell_class = "absdelta"
               if "Relative Delta" in row["Variant"]:
                   try:
                       variant = row["Variant"].replace(" Relative Delta", "")
                       pvalue = pvalue_lookup.get((variant, col), 1)
                       if col not in exclude_from_coloring:
                           clean_value = str(cell_value).replace('$','').replace('%','').replace(',','').strip()
                           numeric_value = float(clean_value) if clean_value else 0
                           if pvalue < SIGNIFICANCE:
                               # Reverse color logic for specific columns
                               if col in reverse_color_columns:
                                   cell_class = "positive" if numeric_value < 0 else "negative"
                               else:
                                   cell_class = "positive" if numeric_value > 0 else "negative"
                   except: pass
               if "P-Value" in row["Variant"]:
                   try:
                       if col not in exclude_from_coloring:
                           clean_value = str(cell_value).replace('$','').replace('%','').replace(',','').strip()
                           if float(clean_value) < SIGNIFICANCE:
                               cell_class = "significant"
                   except: pass
               styled_html += f'<td class="{cell_class}">{cell_value}</td>'
           styled_html += "</tr>"
       styled_html += f"""
</tbody>
</table>
</div>
</div>
</div>"""
       html_outputs.append(styled_html)
   return html_outputs
# Usage remains the same:
# displayHTML(style_summary_table(formatted_df)[0])  # Transaction
# displayHTML(style_summary_table(formatted_df)[1])  # Engagement
# displayHTML(style_summary_table(formatted_df)[2])  # Ecomm

# COMMAND ----------

# DBTITLE 1,Transaction Metrics Display
#Transaction Metrics
displayHTML(style_summary_table(formatted_df)[0])

# COMMAND ----------

# DBTITLE 1,Engagement Metrics Display
# Engagement Metrics  
displayHTML(style_summary_table(formatted_df)[1])

# COMMAND ----------

# DBTITLE 1,Ecomm Metrics Display
# Ecomm Metrics
displayHTML(style_summary_table(formatted_df)[2])

# COMMAND ----------

# DBTITLE 1,Power Display
power_result_df = spark.sql("select * from db_work.POWER_OUTPUT_RESULTS")
experiment_power_df = power_result_df.filter(power_result_df['EXPERIMENT_ID'] == EXPERIMENT_ID).toPandas()
if not experiment_power_df.empty:
   row = experiment_power_df.iloc[0]
   summary_html = f"""
<style>
   .power-table {{
       font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
       border: 1px solid #e0e0e0;
       border-radius: 8px;
       margin: 20px 0;
       box-shadow: 0 1px 3px rgba(0,0,0,0.12);
   }}
   .power-table h2 {{
       color: #2c3e50;
       padding: 15px 20px;
       margin: 0;
       font-size: 18px;
       border-bottom: 1px solid #e0e0e0;
   }}
   .power-table table {{
       width: 100%;
       border-collapse: collapse;
       background: white;
   }}
   .power-table th {{
       background-color: #0056b3;
       color: white;
       padding: 12px 15px;
       text-align: left;
       font-weight: 600;
   }}
   .power-table td {{
       padding: 12px 15px;
       border-bottom: 1px solid #e0e0e0;
   }}
   .power-table tr:nth-child(even) {{
       background-color: #f8f9fa;
   }}
   .power-table .footer {{
       text-align: center;
       color: #666;
       font-size: 12px;
       padding: 10px;
       border-top: 1px solid #e0e0e0;
   }}
</style>
<div class="power-table">
<h2>📊 Sample Size Overview</h2>
<table>
<thead>
<tr>
<th>Metric</th>
<th>Powered on Revenue & AGP</th>
<th>Powered on Deconstructed</th>
</tr>
</thead>
<tbody>
<tr>
<td>Expected Duration</td>
<td>{int(row['POWERED_ON_RPC_DURATION_IN_WEEKS'])} Weeks</td>
<td>{int(row['POWERED_ON_DECONSTRUCTED_DURATION_IN_WEEKS'])} Weeks</td>
</tr>
<tr>
<td>Customers</td>
<td>{int(row['POWERED_ON_RPC_HOUSEHOLDS']):,}</td>
<td>{int(row['POWERED_ON_DECONSTRUCTED_HOUSEHOLDS']):,}</td>
</tr>
<tr>
<td>RPC MDE (%)</td>
<td>{float(row['POWERED_ON_RPC_RPC_MDE_PCT']):.2f}</td>
<td>{float(row['POWERED_ON_DECONSTRUCTED_RPC_MDE_PCT']):.2f}</td>
</tr>
<tr>
<td>AGP MDE (%)</td>
<td>{"N/A" if pd.isna(row['POWERED_ON_AGP_AGP_MDE_PCT']) else f"{float(row['POWERED_ON_AGP_AGP_MDE_PCT']):.2f}"}</td>
<td>{"N/A" if pd.isna(row['POWERED_ON_DECONSTRUCTED_AGP_MDE_PCT']) else f"{float(row['POWERED_ON_DECONSTRUCTED_AGP_MDE_PCT']):.2f}"}</td>
</tr>
<tr>
<td>NZ-RPC MDE (%)</td>
<td>N/A</td>
<td>{float(row['POWERED_ON_DECONSTRUCTED_NZ_RPC_MDE_PCT']):.2f}</td>
</tr>
<tr>
<td>CVR MDE (%)</td>
<td>N/A</td>
<td>{float(row['POWERED_ON_DECONSTRUCTED_CVR_MDE_PCT']):.2f}</td>
</tr>
</tbody>
</table>
<div class="footer">
       Generated by Databricks Dashboard
</div>
</div>
   """
   displayHTML(summary_html)
else:
   error_html = """
<div style="padding: 20px;
               margin: 20px 0;
               border: 1px solid #ff4444;
               border-radius: 8px;
               background-color: #ffebee;
               color: #cc0000;
               font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;">
       ⚠️ No corresponding experiment ID found in the power calculation table
</div>
   """
   displayHTML(error_html)

# COMMAND ----------

def create_dashboard_element(summary_df):
   """Create key metrics summary with guaranteed data population and styling"""
   # ================================
   # Experiment ID Check
   # ================================
   if summary_df.empty or 'Variant' not in summary_df.columns:
       return """<div style='padding: 15px; margin: 20px;
                   border-radius: 5px; background-color: #FFC7CE;
                   color: #9C0006; border: 1px solid #9C0006;'>
                   ⚠️ No corresponding experiment ID found in the power calculation table
</div>"""
   # ================================
   # Define Metrics
   # ================================
   metrics = ['AGP Per Customer', 'Total Revenue Per Customer', 'CVR','Net Sales Per Customer']
   # ================================
   # Data Processing
   # ================================
   df = summary_df.copy().fillna('')
   metrics_data = {}
   pvalue_lookup = {}
   for idx, row in df.iterrows():
       variant = row['Variant']
       if 'Variant A' in variant:
           continue  # Skip control
       # Relative Delta row
       if 'Relative Delta' in variant:
           base_variant = variant.replace(' Relative Delta', '')
           metrics_data[base_variant] = metrics_data.get(base_variant, {})
           for metric in metrics:
               value = str(row[metric]).replace('%', '')
               try:
                   metrics_data[base_variant][metric] = float(value) if value.strip() else ''
               except:
                   metrics_data[base_variant][metric] = ''
       # P-Value row
       if 'P-Value' in variant:
           base_variant = variant.replace(' P-Value', '')
           pvalue_lookup[base_variant] = pvalue_lookup.get(base_variant, {})
           for metric in metrics:
               try:
                   pvalue_lookup[base_variant][metric] = float(row[metric])
               except:
                   pvalue_lookup[base_variant][metric] = 1.0
   # ================================
   # HTML Template & Rendering
   # ================================
   styled_html = """
<style>
   .key-metrics {
       font-family: Arial, sans-serif;
       margin: 20px;
       padding: 20px;
       border-radius: 8px;
       box-shadow: 0 2px 4px rgba(0,0,0,0.1);
       background: white;
   }
   h2 {
       color: #2c3e50;
       font-size: 22px;
       margin-bottom: 20px;
   }
   table {
       border-collapse: collapse;
       width: 100%;
       font-size: 14px;
   }
   th, td {
       border: 1px solid #d9d9d9;
       padding: 12px;
       text-align: center;
       min-width: 120px;
   }
   th {
       background-color: #0056b3 !important;
       color: white !important;
       font-weight: bold;
   }
   .variant-header {
       background-color: #f8f9fa;
       font-weight: bold;
   }
   .positive {
       background-color: #C6EFCE !important;
       color: #006100 !important;
   }
   .negative {
       background-color: #FFC7CE !important;
       color: #9C0006 !important;
   }
</style>
<div class="key-metrics">
<h2>📈 Key Metrics Summary</h2>
<table>
<tr>
<th class="variant-header">Variant</th>"""
   for metric in metrics:
       styled_html += f"<th>{metric} Δ</th>"
   styled_html += "</tr>"
   # ================================
   # Add Variant Rows
   # ================================
   for variant in sorted(metrics_data.keys()):
       styled_html += f"<tr><td class='variant-header'><strong>{variant}</strong></td>"
       for metric in metrics:
           delta = metrics_data[variant].get(metric, '')
           pvalue = pvalue_lookup.get(variant, {}).get(metric, 1.0)
           cell_class = ""
           formatted_value = ""
           if isinstance(delta, (int, float)):
               formatted_value = f"{delta:.1f}%"
               if pvalue < 0.05:
                   cell_class = "positive" if delta > 0 else "negative"
           elif delta != '':
               formatted_value = str(delta)
           styled_html += f"<td class='{cell_class}'>{formatted_value}</td>"
       styled_html += "</tr>"
   styled_html += "</table></div>"
   return styled_html
# Display in Databricks
displayHTML(create_dashboard_element(summary_df))

# COMMAND ----------


Get Outlook for Mac

