use  My_tasks
-- exec sp_rename 'coffe_shop_sales' ,'coffee_shop_sales';

--making a copy of od original table to do our cleaning & analysis just in case
select * 
into copy_coffee_shop_sales
from dbo.coffee_shop_sales

-- verify the table creation
select * from copy_coffee_shop_sales  order by transaction_id; 

--Print column names of the table for reference purpose
declare @col_list nvarchar(max);
select @col_list = STRING_AGG(COLUMN_NAME,', ')
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'copy_coffee_shop_sales';
print @col_list

--Data Cleaning 
-- check from duplicates in transaction_id only because in the table only transaction_id is a unique identifier
	select transaction_id,COUNT(*) as cnt 
	from copy_coffee_shop_sales 
	group by transaction_id
	having COUNT(*) > 1;

	-- Using dynamic sql for duplicates for all columns
	-- Declare variables
		DECLARE @col_name NVARCHAR(255);  -- Variable to hold the column name
		DECLARE @sql NVARCHAR(MAX);       -- Variable to hold the dynamic SQL query
		DECLARE @table_name NVARCHAR(255) = 'copy_coffee_shop_sales'; 

		-- Declare a cursor to iterate through each column
		DECLARE col_cursor CURSOR FOR
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @table_name;

		-- Open the cursor
		OPEN col_cursor;

		-- Fetch the first column name into @col_name
		FETCH NEXT FROM col_cursor INTO @col_name;

		-- Loop through all columns
		WHILE @@FETCH_STATUS = 0
		BEGIN -- Build the dynamic SQL query for the current column
			SET @sql = '
			SELECT ''' + @col_name + ''' AS column_name, COUNT(*) AS dup_cunt
			FROM '+@table_name+'					
			having count(*) > 1;
			';
			-- Print the dynamic SQL query to verify (optional)
			PRINT @sql;

			-- Execute the dynamic SQL query
			EXEC sp_executesql @sql;

			-- Fetch the next column name into @col_name
			FETCH NEXT FROM col_cursor INTO @col_name;
		END

		-- Close and deallocate the cursor
		CLOSE col_cursor;
		DEALLOCATE col_cursor;

-- Checking nulls
		select count(*) as null_count
		from copy_coffee_shop_sales 
		where transaction_date is null;

-- Using dynamic sql for checking null for all columns
	-- Declare variables
		DECLARE @col_name NVARCHAR(255);  -- Variable to hold the column name
		DECLARE @sql NVARCHAR(MAX);       -- Variable to hold the dynamic SQL query
		DECLARE @table_name NVARCHAR(255) = 'copy_coffee_shop_sales'; 

		-- Declare a cursor to iterate through each column
		DECLARE col_cursor CURSOR FOR
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @table_name;

		-- Open the cursor
		OPEN col_cursor;

		-- Fetch the first column name into @col_name
		FETCH NEXT FROM col_cursor INTO @col_name;

		-- Loop through all columns
		WHILE @@FETCH_STATUS = 0
		BEGIN -- Build the dynamic SQL query for the current column
			SET @sql = '
			SELECT ''' + @col_name + ''' AS column_name, COUNT(*) AS null_cnt 
			FROM '+@table_name+'					
			WHERE ' + @col_name + ' IS NULL;
			';
			-- Print the dynamic SQL query to verify (optional)
			PRINT @sql;

			-- Execute the dynamic SQL query
			EXEC sp_executesql @sql;

			-- Fetch the next column name into @col_name
			FETCH NEXT FROM col_cursor INTO @col_name;
		END

		-- Close and deallocate the cursor
		CLOSE col_cursor;
		DEALLOCATE col_cursor;

-- Removing Null rows of transaction_date
delete from copy_coffee_shop_sales
where transaction_date is null;

	/*	 -- incase we have any duplicates in transaction_id column
	insert into copy_coffee_shop_sales (transaction_id,transaction_date,	transaction_time,	transaction_qty,	store_id,	store_location,	product_id,	unit_price,	product_category,	product_type,	product_detail)
	values (26,'01/01/2023','08:33:08',1,5,'Lower Manhattan',	43,	3,	'Tea	Brewed', 'herbal tea',	'Lemon Grass Lg')

	with CTE as (
	select transaction_id, row_number() over (partition by transaction_date,	transaction_time,	transaction_qty,	store_id,	store_location,	product_id,	unit_price,	product_category,	product_type,	product_detail
	order by transaction_id) as rn
	from  copy_coffee_shop_sales)
	delete from copy_coffee_shop_sales
	where exists (select transaction_id,rn from CTE where rn > 1); */

-- Analysis part
-- total sales per month
with sales_CTE as (
	select format(transaction_date,'MMMM') as sale_month,month(transaction_date) as month_num, round(sum(transaction_qty*unit_price),2) as total_sales
	from copy_coffee_shop_sales
	group by format(transaction_date,'MMMM'),month(transaction_date)
	)
select sale_month, total_sales
from sales_CTE
order by month_num;

--month-on-month sales increase/decrease 
with sales_CTE as (
	select
	year(transaction_date) as sale_yr,
	format(transaction_date,'MMMM') as sale_month,
	month(transaction_date) as month_num,
	round(sum(transaction_qty*unit_price),2) as total_sales
	from copy_coffee_shop_sales
	group by format(transaction_date,'MMMM'),month(transaction_date),year(transaction_date)
	)
,order_CTE as (
	select sale_yr, sale_month,month_num,total_sales,
	lag(total_sales) over (order by month_num) as pre_month_sales
	from sales_CTE)
select sale_month,sale_yr, total_sales,
	case when pre_month_sales is null  then 'No sales encountered'
		 else concat(round((total_sales-pre_month_sales)/ pre_month_sales*100,2),'%')
	end as sales_growth_percent
	from order_CTE
	order by sale_yr, month_num; -- incase we have data from mutiple year

--difference in sales between months
with CTE as (
	select 
	year(transaction_date) as sale_yr,
	format(transaction_date,'MMMM') as sale_month,
	month(transaction_date) as month_num,
	round(sum(transaction_qty*unit_price),2) as total_sales
	from copy_coffee_shop_sales
	group by format(transaction_date,'MMMM'),month(transaction_date),year(transaction_date)
)
select sale_month,sale_yr, total_sales,
	   coalesce(total_sales - lag(total_sales) over (order by month_num),0) as sales_difference
from CTE
order by sale_yr, month_num; -- incase we have data from mutiple year

--Total no. of orders per month
with cte as (
		select	sum(transaction_qty) as total_order,
				format(transaction_date,'MMMM') as sale_month,
				month(transaction_date) as month_num
		from copy_coffee_shop_sales
		group by format(transaction_date,'MMMM') , month(transaction_date))
select sale_month, total_order
from cte
order by month_num; -- won't repeat order by year as we know we only have single year data

--month-on-month order increase/decrease 
with order_CTE as (
	select
	format(transaction_date,'MMMM') as order_month,
	month(transaction_date) as month_num,
	sum(transaction_qty) as total_order
	from copy_coffee_shop_sales
	group by format(transaction_date,'MMMM'),month(transaction_date)
	)
,prev_order_CTE as (
	select order_month, month_num,total_order,
	lag(total_order) over (order by month_num) as pre_month_orders
	from order_CTE)
select order_month, total_order,pre_month_orders,
	case when pre_month_orders is null  then 'No order encountered'
		 else concat(round(cast((total_order-pre_month_orders) as decimal(10,2))/ pre_month_orders*100,2),'%')
	end as order_growth_percent
	from prev_order_CTE
	order by month_num; -- incase we have data from mutiple year

--difference in order between months
with CTE as (
	select 
	format(transaction_date,'MMMM') as order_month,
	month(transaction_date) as month_num,
	sum(transaction_qty) as total_order
	from copy_coffee_shop_sales
	group by format(transaction_date,'MMMM'),month(transaction_date)
)
select order_month, total_order,
	   coalesce(total_order - lag(total_order) over (order by month_num),0) as order_difference
from CTE
order by month_num; 

-- Checking on top-selling products
with cte as (
	select format(transaction_date,'MMMM') as sale_month, month(transaction_date) as month_num,
	sum(transaction_qty) as total_qty_sold,
	round(sum(transaction_qty*unit_price),2) as total_sales,
	product_id,product_category,product_type
	from copy_coffee_shop_sales
	group  by  product_id,product_category,product_type,format(transaction_date,'MMMM'),month(transaction_date)
	)
select product_category,sum(total_qty_sold) as total_qty_sold,sum(total_sales) as total_sales,count(product_category)as total_orders
from cte
group  by product_category
order by total_sales desc;

-- Checking on top-selling product & product type by each month based on quantity sold
with sales_cte as (
		select format(transaction_date,'MMMM') as sale_month, month(transaction_date) as month_num,
		sum(transaction_qty) as total_qty_sold,
		product_category,product_type,
		rank() over(partition by format(transaction_date,'MMMM')
		order by round(sum(transaction_qty*unit_price),2) desc) as category_rk
		from copy_coffee_shop_sales
		group by format(transaction_date,'MMMM'),month(transaction_date),product_category,product_type
		)
		,product_category_cte as (
			select sale_month, month_num, product_category,max(total_qty_sold) as max_qty_sold
			from sales_cte
			where category_rk = 1
			group by sale_month,month_num, product_category
		)
		,product_type_cte as (
			select s.sale_month, s.month_num, s.product_category,s.product_type,s.total_qty_sold,
			rank() over(partition by s.sale_month,s.product_category order by s.total_qty_sold desc) as type_rk
			from sales_cte s
			inner join 
			product_category_cte pc on s.sale_month = pc.sale_month 
			and s.product_category = pc.product_category
			)
select pt.sale_month as sale_month,
pt.total_qty_sold as total_qty_sold,
pt.product_category as product_category,
pt.product_type as product_type
from product_type_cte pt
where pt.type_rk = 1
group by pt.sale_month,pt.month_num,pt.total_qty_sold,pt.product_category,pt.product_type
order by pt.month_num;


-- Checking on top-selling product & product type by each month based on revenue
with sales_cte as (
		select format(transaction_date,'MMMM') as sale_month, month(transaction_date) as month_num,
		round(sum(transaction_qty*unit_price),2) as total_sales,
		product_category,product_type,
		rank() over(partition by format(transaction_date,'MMMM')
		order by round(sum(transaction_qty*unit_price),2) desc) as category_rk
		from copy_coffee_shop_sales
		group by format(transaction_date,'MMMM'),month(transaction_date),product_category,product_type
		)
		,product_category_cte as (
			select sale_month, month_num, product_category,max(total_sales) as max_sales
			from sales_cte
			where category_rk = 1
			group by sale_month,month_num, product_category
		)
		,product_type_cte as (
			select s.sale_month, s.month_num, s.product_category,s.product_type,s.total_sales,
			rank() over(partition by s.sale_month,s.product_category order by s.total_sales desc) as type_rk
			from sales_cte s
			inner join 
			product_category_cte pc on s.sale_month = pc.sale_month 
			and s.product_category = pc.product_category
			)
select pt.sale_month as sale_month,
pt.total_sales as total_sales,
pt.product_category as product_category,
pt.product_type as product_type
from product_type_cte pt
where pt.type_rk = 1
group by pt.sale_month,pt.month_num,pt.total_sales,pt.product_category,pt.product_type
order by pt.month_num;

-- sales performance across different store locations
	select store_location,round(sum(transaction_qty*unit_price),2) total_sales--,format(transaction_date,'MMMM')as sale_month,month(transaction_date) as month_num
	from copy_coffee_shop_sales
	group by store_location--,format(transaction_date,'MMMM'),month(transaction_date)
	order by total_sales desc;

-- Top store locations each month based on total sales
with cte as (select format(transaction_date,'MMMM')as sale_month,
			month(transaction_date) as month_num,
			store_location,
			round(sum(transaction_qty*unit_price),2) as total_sales
			from copy_coffee_shop_sales
			group by format(transaction_date,'MMMM'),month(transaction_date),store_location)	
,cte_1 as (
			select sale_month,month_num,store_location,max(total_sales) as max_top_sales,
			rank() over (partition by sale_month order by max(total_sales) desc) as rk
			from cte 
			group by month_num,sale_month,store_location)
select sale_month,store_location,max(max_top_sales) as max_top_sales
from cte_1
where rk =1
group by sale_month,month_num,store_location
order by month_num;

-- Cheking variation in sales by day of the week and top day of the week based on sales
select format(transaction_date,'dddd') as week_day,/*datepart(weekday,transaction_date) as week_num,*/round(sum(transaction_qty*unit_price),2) as total_sales
from copy_coffee_shop_sales
group by format(transaction_date,'dddd')--,datepart(weekday,transaction_date)
order by total_sales desc;

-- Checking hourly variation as well as peak sales hours
select datepart(hour,transaction_time) as day_hour,/*datepart(weekday,transaction_date) as week_num,*/round(sum(transaction_qty*unit_price),2) as total_sales
from copy_coffee_shop_sales
group by datepart(hour,transaction_time)--,datepart(weekday,transaction_date)
order by total_sales desc;
	
