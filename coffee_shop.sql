use coffee_shop;  -- Database instance

-- 1. MOM SALES GROWTH % VIEW
 
if object_id('vw_mom_sales_growth') is not null drop view vw_mom_sales_growth;
go

create view vw_mom_sales_growth
as
with cte1 as 
(
select 
datename(year,transaction_date) as year, 
datename(month,transaction_date) as month,
month(transaction_date) as mnth_num,
sum(sale_amount) as total_sale
from dbo.coffee_shop_sales 
group by datename(year,transaction_date), month(transaction_date), 
datename(month,transaction_date)
)
select year,month,total_sale, coalesce(round(((total_sale - lag(total_sale) 
    over (order by mnth_num))*1.0/lag(total_sale) 
        over (order by mnth_num)) *100.0,2),0) as "MoM%" from cte1;
go
 
 
-- 2. MOM ORDER GROWTH % VIEW

if object_id('vw_mom_order_growth') is not null drop view vw_mom_order_growth;
go

create view vw_mom_order_growth
as
with cte1 as 
(
select 
datename(year,transaction_date) as "year", 
datename(month,transaction_date) as "month",
month(transaction_date) as mnth_num,
sum(transaction_qty) as total_order
from dbo.coffee_shop_sales 
group by datename(year,transaction_date), month(transaction_date), 
datename(month,transaction_date)
)
select 
    "year", 
    "month",
    total_order, 
    coalesce(round(
        ((total_order - lag(total_order) over (order by mnth_num))*1.0/lag(total_order) 
        over (order by mnth_num)),3),
        0
    ) as "MoM%"
from cte1;
go


-- 3. TOTAL SALES & ORDERS BY PRODUCT CATEGORY VIEW

if object_id('vw_category_performance') is not null drop view vw_category_performance;
go

create view vw_category_performance
as
select 
    product_category, 
    sum(sale_amount) as total_sales, 
    sum(transaction_qty) as total_orders 
from 
    dbo.coffee_shop_sales 
group by 
    product_category;
go


-- 4. TOP PRODUCT TYPE PER MONTH (BY REVENUE) VIEW

if object_id('vw_top_product_type_revenue') is not null drop view vw_top_product_type_revenue;
go

create view vw_top_product_type_revenue
as
with cte1 as (
select 
datename(month,transaction_date) as "month", 
month(transaction_date) as mnth_num,
product_type, 
sum(sale_amount) as total_sales,
row_number() over(partition by month(transaction_date) order by sum(sale_amount) desc) as rnk 
from dbo.coffee_shop_sales 
group by month(transaction_date), datename(month,transaction_date), product_type
)
select 
    "month",
    product_type, 
    total_sales
from cte1
where rnk = 1;
go


-- 5. TOP PRODUCT CATEGORY PER MONTH (BY REVENUE) VIEW

if object_id('vw_top_product_category_revenue') is not null 
drop view vw_top_product_category_revenue;
go

create view vw_top_product_category_revenue
as
with cte1 as (
select 
datename(month,transaction_date) as "month", 
month(transaction_date) as mnth_num,
product_category, 
sum(sale_amount) as total_sales,
row_number() over(partition by month(transaction_date) order by sum(sale_amount) desc) as rnk 
from dbo.coffee_shop_sales 
group by month(transaction_date), datename(month,transaction_date), product_category
)
select 
    "month",
    product_category, 
    total_sales
from cte1
where rnk = 1;
go


-- 6. TOP PRODUCT TYPE PER MONTH (BY QUANTITY) VIEW

if object_id('vw_top_product_type_quantity') is not null drop view vw_top_product_type_quantity;
go

create view vw_top_product_type_quantity
as
with cte1 as (
select 
datename(month,transaction_date) as "month", 
month(transaction_date) as mnth_num,
product_category, -- Note: Original query used product_category for product type quantity
sum(transaction_qty) as total_order,
row_number() over(partition by month(transaction_date) order by sum(transaction_qty) desc) as rnk 
from dbo.coffee_shop_sales 
group by month(transaction_date), datename(month,transaction_date), product_category
)
select 
    "month",
    product_category,
    total_order
from cte1
where rnk = 1;
go


-- 7. TOP STORE PER MONTH (BY SALES) VIEW

if object_id('vw_top_store_sales') is not null drop view vw_top_store_sales;
go

create view vw_top_store_sales
as
with cte1 as (
select 
datename(month,transaction_date) as "month", 
month(transaction_date) as mnth_num,
store_location,
sum(sale_amount) as total_sale,
row_number() over(partition by month(transaction_date) order by sum(sale_amount) desc) as rnk 
from dbo.coffee_shop_sales 
group by month(transaction_date), datename(month,transaction_date), store_location
)
select 
    "month",
    store_location,
    total_sale
from cte1
where rnk = 1;
go


-- 8. TOP STORE & IT'S TOP PRODUCT CATEGORY PER MONTH (BY SALES) VIEW

if object_id('vw_top_store_category_sales') is not null drop view vw_top_store_category_sales;
go

create view vw_top_store_category_sales
as
with cte1 as (
select 
datename(month,transaction_date) as "month", 
month(transaction_date) as mnth_num,
store_location,
product_category,
sum(sale_amount) as total_sale,
row_number() over(partition by month(transaction_date) 
order by sum(sale_amount) desc) as rnk1,
row_number() over(partition by month(transaction_date),
store_location order by sum(sale_amount) desc) as rnk2 
from dbo.coffee_shop_sales 
group by month(transaction_date), datename(month,transaction_date), 
store_location, product_category
)
select 
    "month",
    store_location,
    product_category,
    total_sale
from cte1
where rnk1 = 1;
go


-- 9. SALES BY DAY OF WEEK VIEW
-- Can't use SET DATEFIRST 1 in a view. Use ISO formula fornat
-- to guarantee Monday=1 (the intent of the original query).

if object_id('vw_sales_by_day_of_week') is not null drop view vw_sales_by_day_of_week;
go

create view vw_sales_by_day_of_week
as
select 
    ((datepart(weekday, transaction_date) + @@datefirst - 2) % 7) + 1 as week_dy_num,
    datename(weekday,transaction_date) as week_day,
    sum(sale_amount) as total_sale
from dbo.coffee_shop_sales 
group by 
    ((datepart(weekday, transaction_date) + @@datefirst - 2) % 7) + 1,
    datename(weekday,transaction_date);
    /*
        Formula Part	                                Purpose
  DATEPART(WEEKDAY, date)	     Gets the day number based on the current session's start day (@@DATEFIRST).
   + @@DATEFIRST - 2	         This is the offset correction. It mathematically shifts the week number to a 
                                 position where Monday aligns to 0 just before the modulo operation.
        % 7	                     The Modulo operation.This forces the week to cycle correctly from 0 to 6. 
                                 When the number reaches 7 (like Monday in the example), it wraps back to 0.
        + 1	                     The final alignment. This shifts the result from the 0-6 range into the user-friendly 1-7 range, where 1 is Monda
*/
go


-- 10. PEAK HOURS (BY SALES) VIEW

if object_id('vw_peak_hours_by_sales') is not null drop view vw_peak_hours_by_sales;
go

create view vw_peak_hours_by_sales
as
with cte as(
select 
datepart(hour,transaction_time) as hr,
sum(sale_amount) as total_sale
from dbo.coffee_shop_sales 
group by datepart(hh,transaction_time)
)
select hr, 
    case 
		when hr > 12 then concat((hr-12),' PM')
		when hr = 12 then concat(12,' PM')
		when hr = 0 then concat(12, ' AM')
		else concat(hr,' AM')
		end as "12hr",
    total_sale 
from cte;
go
