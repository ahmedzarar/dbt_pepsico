{{ config(materialized="table") }}

select "SPINNEYS" as RetailerName, 
EXTRACT(YEAR FROM PARSE_DATE('%b-%Y', SPLIT(File_Name, ' ')[SAFE_OFFSET(1)])) AS Year,
EXTRACT(MONTH FROM PARSE_DATE('%b-%Y', SPLIT(File_Name, ' ')[SAFE_OFFSET(1)])) AS Month,
Department_Code,Department_Description as srcDepartment,Category_Code,Category_Description as srcCategory,
Sub_Category_Code,Sub_Category_Description as srcSub_Category,BMC_Code,BMC_Description as srcBMC,
'' as Brand_Code,'' as Brand_Principle,Brand_Name as srcBrand,Article_Code,Article_Description as srcSKU
,Article_Barcode as srcBarcode,Site as srcStore,'' as srcStoreCode, Sales__AED_ as Sale_Price
,Sales__Qty_ as Sale_Qty,'' as Material_Group,'' as	Material_Code,'' as	Supplier_Code,'' as	srcSupplier,'' as	Section_Code,	'' as srcSection,'' as	srcChannel,	File_Name,'' as	srcCity
from `pepsico-304512.dbt_Pepsi.SPINNEYS`