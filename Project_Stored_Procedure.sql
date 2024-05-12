-- first we need to check every table for both scehma intermediate and analytics
select * from TPCDS.INTERMEDIATE.CUSTOMER_SCD limit 100;
select * from TPCDS.INTERMEDIATE.DAILY_AGG_SALES limit 100;
select * from TPCDS.ANALYTICS.CUSTOMER_DIM limit 100;
select * from TPCDS.ANALYTICS.DATE_DIM limit 100;
select * from TPCDS.ANALYTICS.ITEM_DIM limit 100;
select * from TPCDS.ANALYTICS.WAREHOUSE_DIM limit 100;
select * from TPCDS.ANALYTICS.WEEKLY_SALES_INV limit 100;


-- as part of the requirement that we need to write the schedual for dims tables and Facts tables the below queries we made the CRON job schedual for both 
-- that schedual the customer_dim in 


------------------------------------------------------Scheduling for customer_dim --------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE TPCDS.ANALYTICS.populating_customer_dimension_using_scd_type_2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
merge into tpcds.intermediate.customer_scd t1 
using TPCDS.RAW.CUSTOMER t2 
on t1.C_SALUTATION = t2.C_SALUTATION
    and t1.C_PREFERRED_CUST_FLAG = t2.C_PREFERRED_CUST_FLAG
    and coalesce(t1.C_FIRST_SALES_DATE_SK ,0) = coalesce(t2.C_FIRST_SALES_DATE_SK, 0)
    and t1.C_CUSTOMER_SK = t2.C_CUSTOMER_SK
    and t1.C_LOGIN = t2.C_LOGIN
    and coalesce(t1.C_CURRENT_CDEMO_SK,0) = coalesce(t2.C_CURRENT_CDEMO_SK,0)
    and t1.C_FIRST_NAME = t2.C_FIRST_NAME
    and coalesce (t1.C_CURRENT_HDEMO_SK,0) = coalesce (t2.C_CURRENT_HDEMO_SK,0)
    and t1.C_CURRENT_ADDR_SK = t2.C_CURRENT_ADDR_SK
    and t1.C_LAST_NAME = t2.C_LAST_NAME
    and t1.C_CUSTOMER_ID = t2.C_CUSTOMER_ID
    and coalesce (t1.C_LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    and coalesce (t1.C_LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    and t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    and coalesce (t1.C_BIRTH_YEAR,0) = coalesce(t2.C_BIRTH_YEAR,0)
    and coalesce (t1.C_BIRTH_DAY,0) = coalesce(t2.C_BIRTH_DAY,0)
    and t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    and coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) = coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
when not matched
then insert (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE
)
values (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    current_date(),
    NULL
);
merge into tpcds.INTERMEDIATE.CUSTOMER_SCD t1 
using tpcds.raw.customer t2 
on t1.C_CUSTOMER_SK = t2.C_CUSTOMER_SK
when matched
and (
    t1.C_SALUTATION!=t2.C_SALUTATION
    OR t1.C_PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR coalesce(t1.C_FIRST_SALES_DATE_SK, 0) != coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    OR t1.C_LOGIN!=t2.C_LOGIN
    OR coalesce(t1.C_CURRENT_CDEMO_SK,0) != coalesce(t2.C_CURRENT_CDEMO_SK,0)
    OR t1.C_FIRST_NAME!=t2.C_FIRST_NAME
    OR coalesce(t1.C_CURRENT_HDEMO_SK,0) != coalesce(t2.C_CURRENT_HDEMO_SK,0)
    OR t1.C_CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.C_LAST_NAME!=t2.C_LAST_NAME
    OR t1.C_CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR coalesce(t1.C_LAST_REVIEW_DATE_SK,0) != coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(t1.C_BIRTH_MONTH,0) != coalesce(t2.C_BIRTH_MONTH,0)
    OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR coalesce(t1.C_BIRTH_YEAR,0) != coalesce(t2.C_BIRTH_YEAR,0)
    OR coalesce(t1.C_BIRTH_DAY,0) != coalesce(t2.C_BIRTH_DAY,0)
    OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) != coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
)
then update set END_DATE = current_date();


create or replace table tpcds.analytics.customer_dim as 
(
select 
    C_SALUTATION,
    C_PREFERRED_CUST_FLAG,
    C_FIRST_SALES_DATE_SK,
    C_CUSTOMER_SK,
    C_LOGIN,
    C_CURRENT_CDEMO_SK,
    C_FIRST_NAME,
    C_CURRENT_HDEMO_SK,
    C_CURRENT_ADDR_SK,
    C_LAST_NAME,
    C_CUSTOMER_ID,
    C_LAST_REVIEW_DATE_SK,
    C_BIRTH_MONTH,
    C_BIRTH_COUNTRY,
    C_BIRTH_YEAR,
    C_BIRTH_DAY,
    C_EMAIL_ADDRESS,
    C_FIRST_SHIPTO_DATE_SK,
    CA_STREET_NAME,
    CA_SUITE_NUMBER,
    CA_STATE,
    CA_LOCATION_TYPE,
    CA_COUNTRY,
    CA_ADDRESS_ID,
    CA_COUNTY,
    CA_STREET_NUMBER,
    CA_ZIP,
    CA_CITY,
    CA_GMT_OFFSET,
    CD_DEP_EMPLOYED_COUNT,
    CD_DEP_COUNT,
    CD_CREDIT_RATING,
    CD_EDUCATION_STATUS,
    CD_PURCHASE_ESTIMATE,
    CD_MARITAL_STATUS,
    CD_DEP_COLLEGE_COUNT,
    CD_GENDER,
    HD_BUY_POTENTIAL,
    HD_DEP_COUNT,
    HD_VEHICLE_COUNT,
    HD_INCOME_BAND_SK,
    IB_LOWER_BOUND,
    IB_UPPER_BOUND,
    START_DATE,
    END_DATE
from tpcds.intermediate.customer_scd cs 
left join tpcds.raw.customer_address ca on cs.c_current_addr_sk = ca.ca_address_sk
left join tpcds.raw.customer_demographics cd on cs.c_current_cdemo_sk = cd.cd_demo_sk
left join tpcds.raw.household_demographics hd on cs.c_current_hdemo_sk = hd.hd_demo_sk
left join tpcds.raw.income_band ib on hd_income_band_sk = ib.ib_income_band_sk
where end_date is null -- here mean that i want the current date 

);
  
  END
  $$;


CREATE OR REPLACE TASK tpcds.analytics.creating_customer_dimension_using_scd_type_2
    WAREHOUSE = BOOTCAMPS
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
    AS
CALL populating_customer_dimension_using_scd_type_2();
alter task TPCDS.ANALYTICS.CREATING_CUSTOMER_DIMENSION_USING_SCD_TYPE_2 resume;
execute task TPCDS.ANALYTICS.CREATING_CUSTOMER_DIMENSION_USING_SCD_TYPE_2;
-- if you want to delete the task and procedure use :
drop task TPCDS.ANALYTICS.CREATING_CUSTOMER_DIMENSION_USING_SCD_TYPE_2;
drop PROCEDURE TPCDS.ANALYTICS."POPULATING_CUSTOMER_DIMENSION_USING_SCD_TYPE_2()";


------------------------------------------------------Scheduling for FACTS table --------------------------------------------------------------------------------


------------- daily_agg_sales 
CREATE OR REPLACE PROCEDURE tpcds.intermediate.populating_daily_agg_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_DATE_SK number;
    BEGIN
        SELECT MAX(SOLD_DATE_SK) INTO :LAST_SOLD_DATE_SK FROM TPCDS.INTERMEDIATE.DAILY_AGG_SALES; 
        delete from tpcds.intermediate.daily_agg_sales where sold_date_sk = :last_sold_date_sk; -- that first should we get 0 and in future wwill be different and will see some recods 
        create or replace table tpcds.intermediate.daily_agg_sales_temp as (
            with increm_sales as (
                select 
                    cs_warehouse_sk as warehouse_sk,
                    cs_item_sk as item_sk ,
                    cs_sold_date_sk as sold_date_sk,
                    cs_quantity as quantity,
                    cs_sales_price * cs_quantity as sales_amt,
                    cs_net_profit as net_profit
                from tpcds.raw.catalog_sales
                where sold_date_sk >= NVL(:last_sold_date_sk,0) 
                and quantity is not null and sales_amt is not null
                union all
                select 
                ws_warehouse_sk as warehouse_sk,
                ws_item_sk as item_sk,
                ws_sold_date_sk as sold_date_sk,
                ws_quantity as quantity,
                ws_sales_price * ws_quantity as sales_amt,
                ws_net_profit as net_profit
                from tpcds.raw.web_sales
                WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
                and quantity is not null
                and sales_amt is not null
            ),
            agg_records_to_dy_s as (
        
                select 
                    warehouse_sk,
                    item_sk,
                    sold_date_sk,
                    sum(quantity) as daily_qnt,
                    sum(sales_amt) as daily_sales_amt,
                    sum(net_profit) as daily_net_profit
                from increm_sales
                group by 1,2,3
            
            ),
            adding_week_number_and_yr_number as
            (
                select 
                    *,
                    date.wk_num as sold_wk_num,
                    date.yr_num as sold_yr_num
                from agg_records_to_dy_s 
                LEFT JOIN tpcds.raw.date_dim date 
                    ON sold_date_sk = d_date_sk
            
            )
        
            
        SELECT 
        	warehouse_sk,
            item_sk,
            sold_date_sk,
            max(sold_wk_num) as sold_wk_num,
            max(sold_yr_num) as sold_yr_num,
            sum(daily_qnt) as daily_qnt,
            sum(daily_sales_amt) as daily_sales_amt,
            sum(daily_net_profit) as daily_net_profit 
        FROM adding_week_number_and_yr_number
        GROUP BY 1,2,3
        ORDER BY 1,2,3
        );
        
        -- now we insert it into tpcds.INTERMEDIATE.DAILY_AGGREGATED_SALES
        INSERT INTO tpcds.intermediate.daily_agg_sales
        (	
            WAREHOUSE_SK, 
            ITEM_SK, 
            SOLD_DATE_SK, 
            SOLD_WK_NUM, 
            SOLD_YR_NUM, 
            daily_qnt, 
            DAILY_SALES_AMT, 
            DAILY_NET_PROFIT
        )
        SELECT 
            DISTINCT
        	warehouse_sk,
            item_sk,
            sold_date_sk,
            sold_wk_num,
            sold_yr_num,
            daily_qnt,
            daily_sales_amt,
            daily_net_profit 
        FROM tpcds.intermediate.daily_agg_sales_temp;

  END
  $$;

CREATE OR REPLACE TASK tpcds.intermediate.creating_daily_agg_sales_incrementally
    WAREHOUSE = BOOTCAMPS
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
    AS
CALL populating_daily_agg_incrementally();
alter task tpcds.intermediate.creating_daily_agg_sales_incrementally resume;
execute task tpcds.intermediate.creating_daily_agg_sales_incrementally;
-- if you want to delete the task and procedure use :
drop task tpcds.intermediate.creating_daily_agg_sales_incrementally;
drop PROCEDURE TPCDS.INTERMEDIATE."POPULATING_DAILY_AGG_INCREMENTALLY()";


------------- weekly_sales_inv

CREATE OR REPLACE PROCEDURE tpcds.analytics.populating_weekly_sales_inv_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_wk_SK number;
    BEGIN
    
    DELETE FROM TPCDS.ANALYTICS.WEEKLY_SALES_INV WHERE sold_wk_sk=:LAST_SOLD_WK_SK;
    SELECT MAX(SOLD_wk_SK) INTO :LAST_SOLD_wk_SK FROM TPCDS.analytics.weekly_sales_inv;

-- compiling all incremental sales records
CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INV_TMP AS (
with agg_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QNT) AS SUM_Qnt_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
    TPCDS.INTERMEDIATE.DAILY_AGG_SALES
GROUP BY
    1,2,4,5
HAVING 
    sold_wk_sk >= NVL(:LAST_SOLD_WK_SK,0)
),

-- We need to have the same sold_wk_sk for all the items. Currently, any items that didn't have any sales on Sunday (first day of the week) would not have Sunday date as sold_wk_sk so this CTE will correct that.
finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_Qnt_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    agg_daily_sales_to_week daily_sales
INNER JOIN TPCDS.RAW.DATE_DIM as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0 -- that 0 mean the first day of week Sunday
),

-- This will help sales and inventory tables to join together using wk_num and yr_num
date_columns_in_inventory_table as (
SELECT 
    inv.*,
    date.wk_num as inv_wk_num,
    date.yr_num as inv_yr_num
FROM
    TPCDS.RAW.INVENTORY inv
INNER JOIN TPCDS.RAW.DATE_DIM as date
on inv.inv_date_sk = date.d_date_sk
)

select 
    warehouse_sk, 
    item_sk, 
    min(SOLD_WK_SK) as sold_wk_sk,
    sold_wk_num as sold_wk_num,
    sold_yr_num as sold_yr_num,
    sum(SUM_Qnt_WK) as SUM_Qnt_WK,
    sum(sum_amt_wk) as sum_amt_wk,
    sum(sum_profit_wk) as sum_profit_wk,
    sum(SUM_Qnt_WK)/7 as avg_qty_dy,
    sum(coalesce(inv.inv_quantity_on_hand, 0)) as inv_qty_wk, 
    sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(SUM_Qnt_WK) as wks_sply,
    iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk,
    sum(sum_amt_wk) / 7 AS avg_daily_amt_wk,
    sum(sum_profit_wk) / 7 AS avg_daily_net_profit_wk

from finding_first_date_of_the_week
left join date_columns_in_inventory_table inv 
    on inv_wk_num = sold_wk_num 
    and inv_yr_num = sold_yr_num 
    and item_sk = inv_item_sk 
    and inv_warehouse_sk = warehouse_sk
group by 1, 2, 4, 5
-- extra precaution because we don't want negative or zero quantities in our final model
having sum(SUM_Qnt_WK) > 0 );

-- now inserting from TPCDS.ANALYTICS.WEEKLY_SALES_INV_TMP into TPCDS.ANALYTICS.WEEKLY_SALES_INV


insert into tpcds.analytics.weekly_sales_inv(
        
    WAREHOUSE_SK,
    ITEM_SK,
    SOLD_WK_SK,
    SOLD_WK_NUM,
    SOLD_YR_NUM,
    SUM_QTY_WK,
    SUM_AMT_WK,
    SUM_PROFIT_WK,
    INV_ON_HAND_QTY_WK,
    WKS_SPLY,
    LOW_STOCK_FLG_WK,
    AVG_DAILY_AMT_WK,
    AVG_DAILY_NET_PROFIT_WK
)
select 
    WAREHOUSE_SK,
    ITEM_SK,
    SOLD_WK_SK,
    SOLD_WK_NUM,
    SOLD_YR_NUM,
    SUM_QNT_WK,
    SUM_AMT_WK,
    SUM_PROFIT_WK,
    INV_QTY_WK,
    WKS_SPLY,
    LOW_STOCK_FLG_WK,
    AVG_DAILY_AMT_WK,
    AVG_DAILY_NET_PROFIT_WK
from tpcds.analytics.weekly_sales_inv_tmp;
    
  END
  $$;

  
CREATE OR REPLACE TASK tpcds.analytics.creating_weekly_sales_inv_incrementally
    WAREHOUSE = BOOTCAMPS
    SCHEDULE = 'USING CRON 0 9 * * 0 UTC'
    AS
CALL populating_weekly_sales_inv_incrementally();
alter task tpcds.analytics.creating_weekly_sales_inv_incrementally resume;
execute task tpcds.analytics.creating_weekly_sales_inv_incrementally;
-- if you want to delete the task and procedure use :
drop task tpcds.intermediate.creating_weekly_sales_inv_incrementally;
drop PROCEDURE TPCDS.ANALYTICS."POPULATING_WEEKLY_SALES_INV_INCREMENTALLY()";

----
