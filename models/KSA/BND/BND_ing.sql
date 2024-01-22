{{ config(materialized='table') }}


select  "BINDAWOOD" as RetailerName,
CAST(CONCAT('20', SUBSTR(CAST(SALES_DATE as STRING), 1, 2)) AS INT64) AS Year,
CAST(SUBSTR(CAST(SALES_DATE as STRING), 3, 2) AS INT64) AS Month,
Cast(null as INT64) as srcPromo_Week,cast(null as string) as srcChannel, Cast(STORE as string) as srcStore,
cast(null as string) as srcStoreName, cast(null as string) as srcCountry,cast(null as string) as srcCity, Cast(null as string) as srcRegion,
CLASS_NAME as srcCategory,Cast(null as string) as srcFlavor, BRAND_NAME as srcBrand,Cast(SKU as string) as srcSKU_Code,
SKU_DESC  as  srcSKU,cast(null as string) as srcManufacturer,cast(null as string) as srcClassName, Cast(SALE_QTY as INT64) as Sale_Qty ,Cast(SALE_AMT as FLOAT64) as Sale_Price,
Cast(null as string) as srcSize, Cast(null as string) as srcUOM,Cast(null as string) as srcPromoOfferCode,Cast(null as string) as srcSizeDesc, Cast(BARCODE as String) as srcBarcode,Cast(VENDOR as string) as srcVendor,
File_Name,Cast(null as string) as Dept_Name,Cast(null as string) as	SupplierName,Cast(null as string) as ItemCode, CAST(Sales_Date as STRING) as Sales_Date  from  `pepsico-304512.dbt_Pepsi.BINDAWOOD` 

UNION ALL

select  "DANUBE" as RetailerName,
CAST(CONCAT('20', SUBSTR(CAST(SALES_DATE as STRING), 1, 2)) AS INT64) AS Year,
CAST(SUBSTR(CAST(SALES_DATE as STRING), 3, 2) AS INT64) AS Month,
Cast(null as INT64) as srcPromo_Week,cast(null as string) as srcChannel, Cast(STORE as string) as srcStore,
cast(null as string) as srcStoreName, cast(null as string) as srcCountry,cast(null as string) as srcCity, Cast(null as string) as srcRegion,
CLASS_NAME as srcCategory,Cast(null as string) as srcFlavor, BRAND_NAME as srcBrand,Cast(SKU as string) as srcSKU_Code,
SKU_DESC  as  srcSKU,cast(null as string) as srcManufacturer,cast(null as string) as srcClassName, Cast(SALE_QTY as INT64) as Sale_Qty ,Cast(SALE_AMT as FLOAT64) as Sale_Price,
Cast(null as string) as srcSize, Cast(null as string) as srcUOM,Cast(null as string) as srcPromoOfferCode,Cast(null as string) as srcSizeDesc, Cast(BARCODE as String) as srcBarcode,Cast(VENDOR as string) as srcVendor,
File_Name,Cast(null as string) as Dept_Name,Cast(null as string) as	SupplierName,Cast(null as string) as ItemCode, CAST(Sales_Date as STRING) as Sales_Date from  `pepsico-304512.dbt_Pepsi.DANUBE` 



