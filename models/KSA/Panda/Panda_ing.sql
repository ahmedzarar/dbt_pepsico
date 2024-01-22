{{ config(materialized='table') }}




select "PANDA" as RetailerName,
CAST(REGEXP_EXTRACT(File_Name, r'\d{4}') as INT64) AS Year,
CAST(Month as INT64) as Month,
CAST(Promo_Week as INT64) as srcPromo_Week,
StoreType as srcChannel,
CAST(Store_Num as STRING) as srcStore,
Store_Name as srcStoreName,
'' as srcCountry,
City as srcCity,
Region as srcRegion,
Sub_Deptartment as srcCategory,
'' as srcFlavor,
Brandname as srcBrand,
CAST(Sku as STRING) as srcSKU_Code,
Description as srcSKU,
'' as srcManufacturer,
Class as srcClassName,
CAST(Qty_TY as INT64) as Sale_Qty,
CAST(Sales_TY_With_VAT as FLOAT64) as Sale_Price,
CAST(Size as STRING) as srcSize,
Selling_U_M as srcUOM,
Promo_Offer_Code as srcPromoOfferCode,
SizeDesc as srcSizeDesc,
CAST(BARCODE as STRING) as srcBarcode,
'' as srcVendor,
File_Name,
'' as Dept_Name,
'' as SupplierName,
'' as ItemCode,
'' as Sales_Date
 from `pepsico-304512.dbt_Pepsi.PANDA` 