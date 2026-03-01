-----Gold Layer----------



-----------Create Dimension: gold.dim_customers

CREATE TABLE gold.dim_customers (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    customer_number NVARCHAR(50),
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    country NVARCHAR(100),
    marital_status NVARCHAR(20),
    gender NVARCHAR(20),
    birthdate DATE,
    create_date DATE
);
INSERT INTO gold.dim_customers (
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date
)
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    la.cntry,
    ci.cst_marital_status,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END,
    ca.bdate,
    ci.cst_create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;




-- Create Dimension: gold.dim_products--


CREATE TABLE gold.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT,
    product_number NVARCHAR(50),
    product_name NVARCHAR(200),
    category_id INT,
    category NVARCHAR(100),
    subcategory NVARCHAR(100),
    maintenance NVARCHAR(50),
    cost DECIMAL(18,2),
    product_line NVARCHAR(50),
    start_date DATE
);

INSERT INTO gold.dim_product (
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date
)
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;




-- Create Fact Table: gold.fact_sales

CREATE TABLE gold.fact_sales (
    order_number NVARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount DECIMAL(18,2),
    quantity INT,
    price DECIMAL(18,2)
);


INSERT INTO gold.fact_sales
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;



