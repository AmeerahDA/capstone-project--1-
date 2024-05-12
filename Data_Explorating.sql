----------------------------------------------------------data exploring -----------------------------------------------------------------------------------------

/*
 * Let's recall some key information about the project data.

Firstly, since we have ingested data from RDS and S3 bucket to Snowflake RAW schema, so please go to Snowflake to explore 
the dataset, especially the fact tables. Please explore the dataset from the following aspects:

Row numbers of each table

Select a single item to determine the frequency of customer orders and how often it is documented in the inventory

How many individual items

How many individual customers

And more...

 * */
-- What information do we have available in tables and how do they connect to each other?


USE TPCDS.raw;
-- FACT table view 
SELECT * FROM TPCDS.RAW.CATALOG_SALES ORDER BY CS_ORDER_NUMBER;
SELECT * FROM TPCDS.RAW.WEB_SALES ORDER BY WS_ORDER_NUMBER;
SELECT * FROM TPCDS.RAW.INVENTORY;
-- Dimenstion table trying to figure out the tables

SELECT * FROM TPCDS.RAW.CATALOG_PAGE;
SELECT * FROM TPCDS.RAW.CUSTOMER ORDER BY C_CUSTOMER_SK;
SELECT * FROM TPCDS.RAW.CUSTOMER_ADDRESS ORDER BY CA_ADDRESS_SK;
SELECT * FROM TPCDS.RAW.CUSTOMER_DEMOGRAPHICS;
SELECT * FROM TPCDS.RAW.DATE_DIM;
SELECT * FROM TPCDS.RAW.HOUSEHOLD_DEMOGRAPHICS;
SELECT * FROM TPCDS.RAW.INCOME_BAND;
SELECT * FROM TPCDS.RAW.ITEM;
SELECT * FROM TPCDS.RAW.PROMOTION;
SELECT * FROM TPCDS.RAW.SHIP_MODE;
SELECT * FROM TPCDS.RAW.TIME_DIM;
SELECT * FROM TPCDS.RAW."WAREHOUSE";
SELECT * FROM TPCDS.RAW.WEB_PAGE;
SELECT * FROM TPCDS.RAW.WEB_SITE;


-- Identify the earliest and latest dates for both sales and inventory records. (
-- you need to join date_dim to see the exact date instead of the date id)
SELECT CAL_DT ,D_DATE_SK FROM TPCDS.RAW.DATE_DIM ORDER BY CAL_DT LIMIT 100;
 -- here we can see the d_date_sk increase WITH cal_dt sence the 1997 was 2,442,410 and 1998-04-06 was 2,442,509

-- ***************** catalog_Sales **********************************

SELECT oldest , earlist 
FROM (
	SELECT CAL_DT AS oldest 
	FROM TPCDS.RAW.DATE_DIM 
	WHERE D_DATE_SK = (SELECT min(CS_SOLD_DATE_SK) AS "oldest"  FROM TPCDS.RAW.CATALOG_SALES) 	
) AS t1 
CROSS JOIN 
(
	SELECT CAL_DT AS earlist 
	FROM TPCDS.RAW.DATE_DIM 
	WHERE D_DATE_SK = (SELECT max(CS_SOLD_DATE_SK) AS "earlist"  FROM TPCDS.RAW.CATALOG_SALES)

) AS t2;

-- ***************** web sales  **********************************

SELECT oldest , earlist 
FROM 
(
	SELECT cal_dt AS oldest 
	FROM TPCDS.RAW.DATE_DIM 
	WHERE d_date_sk = (SELECT min(WS_SOLD_DATE_SK)AS "oldest" FROM TPCDS.RAW.WEB_SALES)
	
) AS t1
CROSS JOIN 
(
	SELECT cal_dt AS earlist
	FROM TPCDS.RAW.DATE_DIM 
	WHERE d_date_sk = (SELECT max(ws_sold_date_sk) AS "earlist" FROM TPCDS.RAW.WEB_SALES)
) AS t2;

-- ***************** inventory  **********************************

SELECT oldest, earlist
FROM 
(
	SELECT cal_dt AS oldest
	FROM TPCDS.RAW.DATE_DIM
	WHERE d_date_sk = (SELECT min(INV_DATE_SK)AS "oldest" FROM TPCDS.RAW.INVENTORY)
) AS t1 
CROSS JOIN 
(
	SELECT cal_dt AS earlist
	FROM TPCDS.RAW.DATE_DIM
	WHERE d_date_sk = (SELECT max(INV_DATE_SK)AS "earlist" FROM TPCDS.RAW.INVENTORY)
) AS t2;



-- Verifying connection to date_dim using cs_sold_date_sk

SELECT 
	cs.CS_SOLD_DATE_SK  , dim.*
FROM 
	TPCDS.RAW.CATALOG_SALES cs INNER JOIN TPCDS.RAW.DATE_DIM dim
	ON dim.D_DATE_SK = cs.CS_SOLD_DATE_SK  
	LIMIT 10;
-- Empty columns CC_REC_END_DATE,CC_STREET_NAME, CC_CLOSED_DATE_SK
-- Verifying connection to catalog pages using cs_catalog_page_sk
SELECT 
	CSALES.CS_CATALOG_PAGE_SK ,CPAGE.*
FROM 
	TPCDS.RAW.CATALOG_SALES csales INNER JOIN TPCDS.RAW.CATALOG_PAGE CPAGE
ON 
	CSALES.CS_CATALOG_PAGE_SK =CPAGE.CP_CATALOG_PAGE_SK 
LIMIT 10;

-- To understand sales and inventory connection, pick up one item to know how frequently it is ordered and recorded in inventory

-- Pick up random item when we find repeatedly in inventory table

SELECT 
	INV_DATE_SK ,INV_ITEM_SK ,count(*) AS counting
FROM 
	TPCDS.RAW.INVENTORY 
GROUP BY 
	1,2
ORDER BY 
	2,1;

-- For inv_item_sk 1, how frequently is it being recorded in the inventory table?

SELECT DISTINCT 
	date.CAL_DT ,date.WK_NUM 
FROM 
	TPCDS.RAW.DATE_DIM date
INNER JOIN 
	TPCDS.RAW.INVENTORY inv 
ON 
	date.D_DATE_SK = inv.INV_DATE_SK 
AND 
	inv.INV_ITEM_SK =1
ORDER BY 
	1,2;


-- How many individual items are there? = 18000
SELECT  
	 count(DISTINCT I_ITEM_SK) AS individual_items
FROM 
	TPCDS.RAW.ITEM 
ORDER BY 1;

---- How many total items do we have = 18000
SELECT 
	count(*) AS Total_items
FROM 
	TPCDS.RAW.ITEM;

-- How many individual customers are there? = 100,000
select
    count(DISTINCT c_customer_sk) AS individual_customers
    -- c_customer_sk,	--CONCAT(c_first_name,' ',c_last_name) AS "Full name" 
from
    TPCDS.RAW.CUSTOMER
ORDER BY 1;
---- What is the count of customers that we have?
select 
	count(distinct c_customer_sk) AS "number of customer" 
from 
	TPCDS.RAW.CUSTOMER;

-- Select a single item to determine the frequency of customer orders and how often it is documented in the inventory
SELECT 
    it.I_ITEM_SK,
    it.I_PRODUCT_NAME,
    COUNT(DISTINCT c.C_CUSTOMER_ID) AS num_order,
    COUNT(DISTINCT inv.inv_item_sk) AS num_inventory
FROM
    TPCDS.RAW.ITEM it
JOIN
    TPCDS.RAW.INVENTORY inv ON it.I_ITEM_SK = inv.INV_ITEM_SK
JOIN
    TPCDS.RAW.CUSTOMER c ON it.I_ITEM_SK = c.C_CUSTOMER_SK
GROUP BY
    it.I_ITEM_SK,
    it.I_PRODUCT_NAME;
    
   
-- as part of data exploring we explor the business requirement and trying o guse the output that we will need it in the future 
   
-- sum_qty_wk: sum(catalog_sales.cs_quantity) group by date_dim.week_num and item, OR sum(web_sales.ws_quantity) group by date_dim.week_num and item
    -- --> Grain: Week Number and Item
SELECT 
   	da.WK_NUM ,
   	it.I_ITEM_SK , it.I_PRODUCT_NAME ,
   	sum(cs.CS_QUANTITY) AS "quantity per week"
FROM 
	TPCDS.RAW.CATALOG_SALES cs 
   JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK 
   JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK =da.D_DATE_SK 
GROUP BY 1,2,3
ORDER BY 1;

-- sum_amt_wk: sum(catalog_sales.cs_sales_price * catalog_sales.cs_quantity) group by date_dim.week_num, item 
-- 					OR sum(web_sales.ws_sales_price * web_sales.ws_quantity) group by date_dim.week_num, item

SELECT 
	da.WK_NUM ,
	it.I_ITEM_SK ,it.I_PRODUCT_NAME ,
	sum(cs.CS_SALES_PRICE * cs.CS_QUANTITY) AS "sum of amount per week"
FROM 
	TPCDS.RAW.CATALOG_SALES cs 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK =it.I_ITEM_SK 
	JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
GROUP BY 1,2,3
ORDER BY 1;

-- *******to test the amount per week ***********
SELECT 
   	da.WK_NUM ,
   	it.I_ITEM_SK , it.I_PRODUCT_NAME ,
   	sum(cs.CS_QUANTITY) AS "quantity per week"
FROM 
	TPCDS.RAW.CATALOG_SALES cs 
   JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK 
   JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK =da.D_DATE_SK 
WHERE 
	it.I_ITEM_SK = 14714 
GROUP BY 1,2,3
ORDER BY 1; -- AS we can see here WITH this iteml_sk we can see IN week 1 the quantity = 42


SELECT 
	da.WK_NUM ,
	it.I_ITEM_SK ,it.I_PRODUCT_NAME ,
	sum(cs.CS_SALES_PRICE) AS "sum of amount per week"
FROM 
	TPCDS.RAW.CATALOG_SALES cs 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK =it.I_ITEM_SK 
	JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
WHERE 
	it.I_ITEM_SK = 14714 
GROUP BY 1,2,3
ORDER BY 1; -- now FOR this item_sk FOR amount per week we can see that = 47.15
-- 				and if we multplie 42 * 47.15 = 1,980.3, so now we now that the sum_amt_wk hold correct forumal 


--  sum_profit_wk: sum(catalog_sales.cs_net_profit) group by date_dim.week_num,
--  item OR sum(web_sales.ws_net_profit) group by date_dim.week_num, item
SELECT 
	da.WK_NUM ,
	it.I_ITEM_SK , it.I_PRODUCT_NAME ,
	sum(cs.CS_NET_PROFIT) AS sum_of_profit_per_week_for_each_items
FROM 
	TPCDS.RAW.CATALOG_SALES cs JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK
GROUP BY 1,2,3
HAVING 
	sum_of_profit_per_week_for_each_items > 0
ORDER BY 
	sum_of_profit_per_week_for_each_items desc; -- here FOR calculating the net profit we have a negative number 
-- when we order it as ASC it's will show u and when we add:
-- HAVING sum_of_profit_per_week_for_each_items > 0 we can see only the positive number !!
 

-- avg_qty_dy: = sum_qty_wk/7
SELECT 	
	da.WK_NUM ,
	it.I_ITEM_SK , it.I_PRODUCT_NAME ,
	(sum(cs.CS_QUANTITY) / 7) AS avg_qnt_per_day
FROM 
	TPCDS.RAW.CATALOG_SALES cs JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK
GROUP BY 1,2,3
HAVING 
	avg_qnt_per_day >0
ORDER BY avg_qnt_per_day desc;

-- inv_on_hand_qty_wk: inventory.inv_quantity_on_hand at date_dim.week_num, warehouse
SELECT 
	da.WK_NUM , 
	wh.W_WAREHOUSE_SK ,wh.W_WAREHOUSE_NAME ,
	sum(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qnt_per_week
FROM 
	TPCDS.RAW.INVENTORY inv JOIN TPCDS.RAW.DATE_DIM da ON inv.INV_DATE_SK = da.D_DATE_SK 
JOIN 
	TPCDS.RAW."WAREHOUSE" wh ON inv.INV_WAREHOUSE_SK = wh.W_WAREHOUSE_SK
GROUP BY 1,2,3 ORDER BY 4 desc;
-- wks_sply: = inv_on_hand_qty_wk/sum_qty_wk : there are two method to write it 
-- method 1 using JOin
SELECT 
	da.WK_NUM ,
	it.I_ITEM_SK ,it.I_PRODUCT_NAME ,
	(sum(inv.INV_QUANTITY_ON_HAND) / sum(cs.CS_QUANTITY))AS wks_sply
FROM 
	TPCDS.RAW.CATALOG_SALES cs JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK
	JOIN TPCDS.RAW.INVENTORY inv ON inv.INV_DATE_SK = da.D_DATE_SK AND inv.INV_ITEM_SK  = it.I_ITEM_SK 
GROUP BY 1,2,3
HAVING wks_sply > 0
ORDER BY 4 desc;-- IN this query there ARE SOME NULLS VALUES TO aviod it OR IF you don'T want TO see it IN your query ADD HAVING CONDITION 

-- method 2 using Subquery
SELECT 
	combine.wk_num,
	combine.I_ITEM_SK,
	combine.I_PRODUCT_NAME,
	combine.quantity_per_week,
	combine.inv_on_hand_qnt_per_week,
	(combine.inv_on_hand_qnt_per_week / combine.quantity_per_week) AS wks_sply
FROM 
(
SELECT 
    sales.wk_num,
    sales.I_ITEM_SK,
    sales.I_PRODUCT_NAME,
    sales.quantity_per_week,
    inv.inv_on_hand_qnt_per_week
FROM 
    (
        SELECT 
            da.WK_NUM ,
            it.I_ITEM_SK ,
            it.I_PRODUCT_NAME ,
            SUM(cs.CS_QUANTITY) AS quantity_per_week
        FROM 
            TPCDS.RAW.CATALOG_SALES cs 
        JOIN 
            TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK 
        JOIN 
            TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
        GROUP BY 
            1,2,3
        ORDER BY 
            1
    ) sales
JOIN 
    (
        SELECT 
            da.WK_NUM , 
            wh.W_WAREHOUSE_SK ,
            wh.W_WAREHOUSE_NAME ,
            SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qnt_per_week
        FROM 
            TPCDS.RAW.INVENTORY inv 
        JOIN 
            TPCDS.RAW.DATE_DIM da ON inv.INV_DATE_SK = da.D_DATE_SK 
        JOIN 
            TPCDS.RAW."WAREHOUSE" wh ON inv.INV_WAREHOUSE_SK = wh.W_WAREHOUSE_SK
        GROUP BY 
            1,2,3 
        ORDER BY 
            4 DESC
    ) inv ON sales.wk_num = inv.wk_num
)combine
ORDER BY 
    6 desc;
-- low_stock_flg_wk: ((avg_qty_dy > 0 && ((avg_qty_dy) > (inventory_on_hand_qty_wk))
  				-- method 1 
SELECT 
	(SUM(cs.CS_QUANTITY) / 7) avg_qnt_per_day,
	SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qnt_per_week,
	((avg_qnt_per_day) > 0 AND (avg_qnt_per_day) > (inv_on_hand_qnt_per_week)) AS low_stock_flg_wk
FROM 
	TPCDS.RAW.CATALOG_SALES cs JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK
	JOIN TPCDS.RAW.INVENTORY inv ON inv.INV_DATE_SK = da.D_DATE_SK AND inv.INV_ITEM_SK =it.I_ITEM_SK ;

				-- method 2 using case statement 
SELECT  
	(SUM(cs.CS_QUANTITY) / 7) avg_qnt_per_day,
	SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qnt_per_week,
	CASE
		WHEN ((avg_qnt_per_day) > 0 AND (avg_qnt_per_day) > (inv_on_hand_qnt_per_week)) THEN TRUE 
		ELSE FALSE 
	END AS low_stock_flg_wk
FROM 
	TPCDS.RAW.CATALOG_SALES cs JOIN TPCDS.RAW.DATE_DIM da ON cs.CS_SOLD_DATE_SK = da.D_DATE_SK 
	JOIN TPCDS.RAW.ITEM it ON cs.CS_ITEM_SK = it.I_ITEM_SK
	JOIN TPCDS.RAW.INVENTORY inv ON inv.INV_DATE_SK = da.D_DATE_SK AND inv.INV_ITEM_SK =it.I_ITEM_SK ;

-- --> In order to understand inventory better, we should have warehouse as a grain as well
-- Integrate Customer Dimension: Customer(SCD Type 2) + Customer_Address + Customer_Demographics + Household_Demographics + Income_Band
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

