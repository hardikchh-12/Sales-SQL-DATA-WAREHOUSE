-- DATA CLEANING--
--Checking for Duplicates or Null in primary key


select cst_id, count(*) as cnt from bronze.crm_cust_info 
group by cst_id having count(*)>1 
order by cst_id

select * from bronze.crm_cust_info where cst_id='29466'

select * from bronze.crm_cust_info where cst_firstname!= trim(cst_firstname)

with cte as 
(select * ,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
from bronze.crm_cust_info where cst_id is not null) 
select * from cte where rnk=1 
--and cst_id='29466'


--Cleaned CRM CUST INFO

Insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

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
from cte where rnk=1

select * from silver.crm_cust_info

select cst_id, count(*) as cnt from silver.crm_cust_info 
group by cst_id having count(*)>1 
order by cst_id
-------------prd info-------------------


select top(1000) * from bronze.crm_prd_info


--------Duplicates and other ----
select prd_id, count(*) as cnt from bronze.crm_prd_info 
group by prd_id having count(*)>1  or prd_id is null
--
select distinct prd_line from bronze.crm_prd_info
select * from bronze.crm_prd_info where prd_cost<0 or prd_cost is null
select * from bronze.crm_prd_info where  prd_start_dt is null

select prd_id,replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id ,
 SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key ,prd_nm,
isnull(prd_cost,0) as prd_cost,
prd_line,cast(prd_start_dt as date )as prd_start_dt ,
cast((lead(prd_start_dt) over(partition by prd_nm order by prd_id )-1) as date) as  prd_end_dt  
from bronze.crm_prd_info
order by prd_id


select * from bronze.crm_cust_info
where cst_id not in (select sls_cust_id from bronze.crm_sales_details)


SELECT *
FROM bronze.crm_cust_info c
WHERE NOT EXISTS (
    SELECT 1
    FROM bronze.crm_sales_details s
    WHERE s.sls_cust_id = c.cst_id
);

select * from silver.crm_prd_info where prd_end_dt<prd_start_dt


------------------------------------------------

-----------Silver.crm_sales_details--------------


select top(1000) * from bronze.crm_sales_details


select SUBSTRING(cast(sls_order_dt as nvarchar),1,4) +'-'+SUBSTRING(cast(sls_order_dt as nvarchar),5,2)+'-'+SUBSTRING(cast(sls_order_dt as nvarchar),7,2) from bronze.crm_sales_details


select sls_ord_num, count(*) as cnt from bronze.crm_sales_details
group by sls_ord_num having count(*)>1 

select * from bronze.crm_sales_details where sls_ord_num = 'SO55367'




select * from bronze.crm_sales_details where sls_order_dt is null or sls_order_dt=0


select len(sls_order_dt),len(sls_ship_dt),len(sls_due_dt) from bronze.crm_sales_details 
where len(sls_order_dt)<8 or len(sls_due_dt)<8 or len(sls_ship_dt)<8 


select * from bronze.crm_sales_details where sls_order_dt<19000101 or sls_order_dt>20500101

select max(sls_order_dt),max(sls_ship_dt),max(sls_due_dt) from bronze.crm_sales_details

select * from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_ship_dt>sls_due_dt

select * from bronze.crm_sales_details where sls_sales!=sls_price*sls_quantity or 
sls_sales is null or sls_price is null or sls_quantity<=0 or sls_quantity is null



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



-------------silver.erp_   -------------------------


select * from bronze.erp_cust_az12 

select * from bronze.erp_cust_az12 where CID is null

select GEN,COUNT(*) from bronze.erp_cust_az12 group by GEN

select distinct GEN from bronze.erp_cust_az12 group by GEN

---duplicates--
with cte as(
select *,DENSE_RANK() over(partition by CID,BDATE,GEN order by CID) as rnk from bronze.erp_cust_az12)
select * from cte where rnk=1

/* No duplicates*/

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
end as Gender --Data Normalization--

from bronze.erp_cust_az12

----------------silver.erp_loc_a101------

select * from bronze.erp_loc_a101

select distinct cntry from bronze.erp_loc_a101

select 
REPLACE(CID,'-','') as CID,
case when Trim(CNTRY) is null or Trim(cntry)='' then 'n/a'
	when TRIM(CNTRY) in ('US','USA','United States') then 'United States'
	when trim(CNTRY) = 'DE' then 'Germany'
	else trim(cntry) --Normalization and Handling null --
end as CNTRY
from bronze.erp_loc_a101



--------------silver.erp_px_cat_g1v2------------


select * from bronze.erp_px_cat_g1v2





