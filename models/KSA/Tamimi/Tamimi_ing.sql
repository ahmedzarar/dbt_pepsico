{{ config(materialized='table') }}

SELECT 'TAMIMI' as RetailerName, CAST(REGEXP_EXTRACT(File_Name, r'\d{4}') as INT64) AS Year,
CAST( CASE
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)January|Jan') THEN 1
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)February|Feb') THEN 2
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)March|Mar') THEN 3
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)April|Apr') THEN 4
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)May') THEN 5
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)June|Jun') THEN 6
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)July|Jul') THEN 7
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)August|Aug') THEN 8
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)September|Sep') THEN 9
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)October|Oct') THEN 10
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)November|Nov') THEN 11
    WHEN REGEXP_CONTAINS(File_Name, r'(?i)December|Dec') THEN 12
    ELSE NULL
  END as INT64) as Month,
CAST(NULL as INT64) as srcPromo_Week,
'' as srcChannel,
TRIM(Store) as srcStore,
-- SUBSTR(Store, INSTR(Store, ' ') + 1)as srcStore,
'' as srcStoreName,
'' as srcCountry,
'' as srcCity,
'' as srcRegion,
Merch_cat as srcCategory,
'' as srcFlavor,
Trim(Brand) as srcBrand,

CAST(Article as STRING) as srcSKU_Code,
Trim(`Desc`) AS srcSKU,
'' as srcManufacturer,
'' as srcClassName,
CAST(Qty as INT64) as Sale_Qty,
CAST(Sales as FLOAT64) as Sale_Price,
'' as srcSize,
UOM as srcUOM,
'' as srcPromoOfferCode,
'' as srcSizeDesc,
'' as srcBarcode,
'' as srcVendor,
File_Name,
'' as Dept_Name,
'' as SupplierName,
'' as ItemCode,
'' as Sales_Date
FROM `pepsico-304512.dbt_Pepsi.TAMIMI`