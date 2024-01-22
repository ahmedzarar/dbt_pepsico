{{ config(materialized='table') }}


select  "OTHAIM" as RetailerName,
CAST(REGEXP_EXTRACT(File_Name, r'-(\d{4})$') as INT64) AS Year,
CAST(REGEXP_EXTRACT(File_Name, r'-(\d{2})-') as INT64) AS Month,
NUll as srcPromo_Week,
 IF(REGEXP_CONTAINS(LOWER(Store_Format_English_Description), r'store format english'), 'Channel', Store_Format_English_Description) AS srcChannel,CAST(STORE_NUMBER as STRING) as srcStore,
cast(null as string) as srcStoreName,cast(null as string)	as srcCountry,cast(null as string)	as srcCity,cast(null as string)	as srcRegion, D3_SUB_DEPARTMENT_NAME as srcCategory,
D4_CLASS_NAME_WITH_NUMBER as srcFlavor,D5_SUB_CLASS_NAME_WITH_NUMBER as srcBrand, CAST(SKU as STRING) as srcSKU_Code, ITEM_ENGLISH_DESCRIPTION as srcSKU, VENDOR_NAME_WITH_NUMBER as srcManufacturer ,
D2_DEPARTMENT_NAME as srcClassName,Sales_Quantity as Sale_Qty,Sales_Amount as Sale_Price,
cast(null as string) as srcSize,cast(null as string) as	srcUOM,cast(null as string) AS srcPromoOfferCode,cast(null as string) as srcSizeDesc,cast(null as string) as srcBarcode,cast(null as string) as	srcVendor,File_Name,
cast(null as string) as	Dept_Name,cast(null as string) as	SupplierName,cast(null as string) as	ItemCode,cast(null as string) as	Sales_Date

 from `pepsico-304512.dbt_Pepsi.OTHAIM` where
  STORE_NUMBER is not Null