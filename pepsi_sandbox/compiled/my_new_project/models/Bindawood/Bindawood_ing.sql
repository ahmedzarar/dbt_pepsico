


select  "BINDAWOOD" as RetailerName,
 CAST(CONCAT('20', SUBSTR(CAST(SALES_DATE as STRING), 1, 2)) AS INT64) AS Year,
  CAST(SUBSTR(CAST(SALES_DATE as STRING), 3, 2) AS INT64) AS Month,
  '' as srcPromo_Week,'' as srcChannel, STORE as srcStore,
'' as srcStoreName, '' as	srcCountry,'' as srcCity, '' as	srcRegion,
CLASS_NAME as srcCategory,'' as srcFlavor, BRAND_NAME as srcBrand,SKU as srcSKU_Code,
 SKU_DESC  as  srcSKU,'' as srcManufacturer,'' as	srcClassName, SALE_QTY,SALE_AMT as Sale_Price,
'' as srcSize, '' as	srcUOM,'' as srcPromoOfferCode,'' as srcSizeDesc, BARCODE as srcBarcode,VENDOR as srcVendor,
File_Name,'' as Dept_Name,'' as	SupplierName,''	as ItemCode, Sales_Date

 from `pepsico-304512.dbt_Pepsi.BINDAWOOD`