
----------------------------------------------------------DATA Modeling  -----------------------------------------------------------------------------------------
-- as the requirement mension that we need to create two schemas intermidate schema and that will DE have access into and analytics schema that will business intelligence and DA and DS teams have access into it , after that we need to build the structure for both schemas. 

-- intermidate schema will have customer_scd type 2 and daily_agg_sales : in customer_scd will have same raw.customer but we will add start_date and end_date to follow the Slowly Changing Dimension Type 2 prictices  then we will join the other tables Customer_Address + Customer_Demographics + Household_Demographics + Income_Band when we transform the customer_scd into analytics.customer_dim table 
-- for daily_agg_sales we will combine or let's we say we will use union all for both web_sales and catalog_sales into one table that called (daily_agg_sales) and this table we will keep it for future if they ask and they wanttable that repreent the daily sales , also we can use that in analytics.weekly_sales_inv in future 
-- adding scehma 
CREATE OR REPLACE SCHEMA INTERMEDIATE; 
CREATE OR REPLACE SCHEMA ANALYTICS;

USE SCHEMA intermediate;

CREATE OR REPLACE TABLE customer_SCD (
	C_SALUTATION VARCHAR(16777216),
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),
	C_FIRST_SALES_DATE_SK NUMBER(38,0),
	C_CUSTOMER_SK NUMBER(38,0),
	C_LOGIN VARCHAR(16777216),
	C_CURRENT_CDEMO_SK NUMBER(38,0),
	C_FIRST_NAME VARCHAR(16777216),
	C_CURRENT_HDEMO_SK NUMBER(38,0),
	C_CURRENT_ADDR_SK NUMBER(38,0),
	C_LAST_NAME VARCHAR(16777216),
	C_CUSTOMER_ID VARCHAR(16777216),
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),
	C_BIRTH_MONTH NUMBER(38,0),
	C_BIRTH_COUNTRY VARCHAR(16777216),
	C_BIRTH_YEAR NUMBER(38,0),
	C_BIRTH_DAY NUMBER(38,0),
	C_EMAIL_ADDRESS VARCHAR(16777216),
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	start_date timestamp(9),
	end_date timestamp(9)
);


CREATE OR REPLACE TABLE tpcds.INTERMEDIATE.daily_agg_sales (
	warehouse_sk int,
	item_sk int,
	sold_date_sk int,
	sold_wk_num int,
	sold_yr_num int,
	daily_qnt int,
	daily_sales_amt float,
	daily_net_profit float);

USE SCHEMA analytics;

CREATE OR REPLACE TABLE tpcds.ANALYTICS.customer_dim(
	SALUTATION VARCHAR(16777216),
	PREFERRED_CUST_FLAG VARCHAR(16777216),
	FIRST_SALES_DATE_SK NUMBER(38,0),
	CUSTOMER_SK NUMBER(38,0),
	LOGIN VARCHAR(16777216),
	CURRENT_CDEMO_SK NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	CURRENT_HDEMO_SK NUMBER(38,0),
	CURRENT_ADDR_SK NUMBER(38,0),
	LAST_NAME VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	LAST_REVIEW_DATE_SK NUMBER(38,0),
	BIRTH_MONTH NUMBER(38,0),
	BIRTH_COUNTRY VARCHAR(16777216),
	BIRTH_YEAR NUMBER(38,0),
	BIRTH_DAY NUMBER(38,0),
	EMAIL_ADDRESS VARCHAR(16777216),
	FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	STREET_NAME VARCHAR(16777216),
	SUITE_NUMBER VARCHAR(16777216),
	STATE VARCHAR(16777216),
	LOCATION_TYPE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	ADDRESS_ID VARCHAR(16777216),
	COUNTY VARCHAR(16777216),
	STREET_NUMBER VARCHAR(16777216),
	ZIP VARCHAR(16777216),
	CITY VARCHAR(16777216),
	GMT_OFFSET FLOAT,
	DEP_EMPLOYED_COUNT NUMBER(38,0),
	DEP_COUNT NUMBER(38,0),
	CREDIT_RATING VARCHAR(16777216),
	EDUCATION_STATUS VARCHAR(16777216),
	PURCHASE_ESTIMATE NUMBER(38,0),
	MARITAL_STATUS VARCHAR(16777216),
	DEP_COLLEGE_COUNT NUMBER(38,0),
	GENDER VARCHAR(16777216),
	BUY_POTENTIAL VARCHAR(16777216),
	hd_DEP_COUNT NUMBER(38,0), -- FOR customer demograghoc AND haouseholder ALL two TABLE have dept_count AND here hd_dept_count FOR household tbale 
	VEHICLE_COUNT NUMBER(38,0),
	INCOME_BAND_SK NUMBER(38,0),
	LOWER_BOUND NUMBER(38,0),
	UPPER_BOUND NUMBER(38,0),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9)
);


CREATE OR REPLACE TABLE tpcds.ANALYTICS.weekly_sales_inv (
	WAREHOUSE_sk int,
	item_sk int,
	sold_wk_sk int,
	sold_wk_num int ,
	sold_yr_num int ,
	sum_qty_wk int ,
	sum_amt_wk float , 
	sum_profit_wk float ,
	avg_qty_dy int ,
	avg_daily_amt_wk int,
	avg_daily_net_profit_wk int,
	inv_on_hand_qty_wk int ,
	wks_sply int ,
	low_stock_flg_wk boolean
);


-------------------------cloing  itme and warehouse and date from raw-------------------------------------------------------------------

-- we need to integrate these table with customer_dim so we colne it as it same 
CREATE OR REPLACE TABLE tpcds.ANALYTICS.item_dim CLONE tpcds.RAW.ITEM ;
CREATE OR REPLACE TABLE tpcds.ANALYTICS.warehouse_dim CLONE tpcds.RAW."WAREHOUSE";
CREATE OR REPLACE TABLE tpcds.ANALYTICS.date_dim CLONE tpcds.RAW.DATE_DIM ;





-------------------------loading data from raw.customer into intermdate.customer_dim-----------------------------------------------------

-- first we need to understand each columns and need to know that each columns require is there 
select * from TPCDS.INTERMEDIATE.DAILY_AGG_SALES;
select * from TPCDS.INTERMEDIATE.CUSTOMER_SCD;
-- here why we use merge ? --> merge statement we use it to preform the upsert operation and upsert operation mean 
-- database operation that performs an update if a record exists, or an insert if it does not exist
-- sp it's preform the upsert operation on tpcds.intermedate.customer_scd table using data from the tpcds.raw.customer 
-- so will combine both functionatly insert and update staement and allowing you to insert new records or update existing record based on matched condition .
-- And if found matching record in t1 based on the join condition the existing records will be updated with the values of t2 , 
-- The WHEN NOT MATCHED clause defines the actions to be performed on rows that do not have a matching row in t1. In this case, it inserts new rows into t1 using the values from t2.
-- if it not matching the records so will be inserting into t1 with the values of t2
-- the start_date here it's using the current_date for new records that are inserts into table and END_DATE will be nulls for new customer untill the customer records is updated or terminated in subsequent operation 
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

select * from TPCDS.INTERMEDIATE.CUSTOMER_SCD where c_current_cdemo_sk= 980124;
select * from TPCDS.ANALYTICS.CUSTOMER_DIM where c_current_cdemo_sk= 980124;
 -- now we finish -> Develop a merge script to integrate the new Customer dimension table into the existing dimension table within the Analytics schema, following Type 2 methodology.

 ------------for FACT table we will use INCREMENTIAL stratgey 
 -- what is theat mean ->> so first we initial load everything and try to get the max_date and delete it , the second load will be incremntial and that mean the insert should be bigger than the max_date insert>= max_date 


  -- in data expolartion part we nitce that we doesn;t have max date but we know the we have sold_date_sk that will increase when the date is increase so 
-- why we need to set variable then delete it ? so setting the LAST_SOLD_DATE_SK variable and deleting partial records from the last date the ensure we have only incremential records include it in our daily_agg_sales and avioding for any potencial duplication 

  -- ------------------------------for daily_agg_sales fact table 
set last_sold_date_sk = (select max(sold_date_sk) from tpcds.intermediate.daily_agg_sales);
delete from tpcds.intermediate.daily_agg_sales where sold_date_sk = $last_sold_date_sk; -- that first should we get 0 and in future wwill be different and will see some recods 
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
        where sold_date_sk >= NVL($last_sold_date_sk,0) 
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
        WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
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



-------------------- for weekly_sales_inv fact table ----------------------------------------------------------------------
SET LAST_SOLD_WK_SK = (SELECT MAX(SOLD_WK_SK) FROM TPCDS.ANALYTICS.WEEKLY_SALES_INV);

-- Removing partial records from the last date
DELETE FROM TPCDS.ANALYTICS.WEEKLY_SALES_INV WHERE sold_wk_sk=$LAST_SOLD_WK_SK;


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
    sold_wk_sk >= NVL($LAST_SOLD_WK_SK,0)
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


-- now we need to check the data inside table of weekly_sales_inv 
select * from TPCDS.ANALYTICS.WEEKLY_SALES_INV;