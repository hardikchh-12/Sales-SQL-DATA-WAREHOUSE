

--Creating Procedure

Create or alter PROCEDURE silver.load_silver
as
BEGIN

-- Adding data to Silver Layer

---- Silver.crm_cust_info----

	truncate table Silver.crm_cust_info;

	with cte as 
	(select * ,
	ROW_NUMBER() over(partition by cst_id order by cst_create_date) as rnk
	from bronze.crm_cust_info where cst_id is not null) 

	Insert into silver.crm_cust_info(
	cst_id,cst_key,cst_firstname,cst_lastname,
	cst_marital_status,cst_gndr,cst_create_date
	)

	select cst_id,cst_key,trim(cst_firstname) as cst_firstname, 
	trim(cst_lastname) as cst_lastname,
	case
		when cst_marital_status='S' then 'Single'
		when cst_marital_status='M' then 'Married'
		else 'n/a'
	end as cst_marital_status,
	case
		when cst_gndr='F' then 'Female'
		when cst_gndr='M' then 'Male'
		when cst_gndr is null then 'N/A'
	end as cst_gndr,cst_create_date
	from cte where rnk=1;


	--------------------------

	-----silver.crm_prod_info--------------
	truncate table Silver.crm_prd_info;

	insert into silver.crm_prd_info(
	prd_id,cat_id,prd_key,prd_nm,prd_cost,
	prd_line,prd_start_dt,prd_end_dt)

	select prd_id,replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id ,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key ,prd_nm,
	isnull(prd_cost,0) as prd_cost,
	prd_line,cast(prd_start_dt as date )as prd_start_dt ,
	cast((lead(prd_start_dt) over(partition by prd_nm order by prd_id )-1) as date) as  prd_end_dt  
	from bronze.crm_prd_info
	order by prd_id

	--select * from silver.crm_prd_info




	-----------Silver.crm_sales_details--------------

	truncate table Silver.crm_sales_details;


	Insert into silver.crm_sales_details(
	sls_ord_num,sls_prd_key,sls_cust_id,
	sls_order_dt,sls_ship_dt,sls_due_dt,
	sls_sales,sls_quantity,sls_price)


	select sls_ord_num,sls_prd_key,sls_cust_id, 
	case when sls_order_dt=0 or len(sls_order_dt)!=8 then Null
		else TRY_CONVERT(date,cast(nullif (sls_order_dt,0) as nvarchar(8)))
	end as sls_order_dt,
	case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then Null
		else TRY_CONVERT(date,cast(nullif(sls_ship_dt,0) as nvarchar(8)))
	end as sls_ship_dt,
	case when sls_due_dt=0 or len(sls_due_dt)!=8 then Null
		else TRY_CONVERT(date,cast(nullif(sls_due_dt,0) as nvarchar(8)))
	end as sls_due_dt,
	case when sls_sales is null or sls_sales<=0 or sls_sales!=abs(sls_price)*sls_quantity
		then sls_quantity*abs(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price<=0 
		then sls_sales/nullif(sls_quantity,0)
		else sls_price
	end as sls_price
	from bronze.crm_sales_details 

	--select * from silver.crm_sales_details

	------ silver.erp_cust_az12

	truncate table silver.erp_cust_az12;

	insert into silver.erp_cust_az12(
	CID,BDATE,GEN)

	select 
	case when CID like 'NAS%' then TRIM(SUBSTRING(CID,4,len(CID)))
		else TRIM(CID) 
	end as Cust_key,
	case when bdate > GETDATE() then NULL
		else BDATE
	end as BDATE,
	CASE 
		when Trim(GEN)='F' or trim(GEN)='Female' then 'Female'
		when trim(GEN)='M' or trim(GEN)='Male' then 'Male'
		else 'n/a'
	end as Gender

	from bronze.erp_cust_az12


	--select * from silver.erp_cust_az12


	-----------silver.erp_loc_a101-------------

	truncate table silver.erp_loc_a101;

	insert into silver.erp_loc_a101(CID,CNTRY)

	select 
	REPLACE(CID,'-','') as CID,
	case when Trim(CNTRY) is null or Trim(cntry)='' then 'n/a'
		when TRIM(CNTRY) in ('US','USA','United States') then 'United States'
		when trim(CNTRY) = 'DE' then 'Germany'
		else trim(cntry)
	end as CNTRY
	from bronze.erp_loc_a101

	--select * from silver.erp_loc_a101

	------------- silver.erp_px_cat_g1v2 ----------------

	truncate table silver.erp_px_cat_g1v2

	insert into silver.erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
	select * from bronze.erp_px_cat_g1v2


	--select * from silver.erp_px_cat_g1v2


END
