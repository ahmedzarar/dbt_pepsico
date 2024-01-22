{{ config(materialized='table') }}
with panda_ing as (select distinct srcSKU_Code,srcSKU,RetailerName from {{ ref('Panda_ing') }} )
,
sku as (
select * from panda_ing SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku SKU
ON SELLOUT.srcSKU_Code = SKU.SKU_CODE
and SELLOUT.RetailerName=SKU.Retailer
)



select RetailerName,srcSKU_Code,srcSKU,Category,Manufacturer,Brands,Sub_Brands,	Pack_Type,	Pack_Size,	Pack_Size_Range,	Pack_quantity,	Raw_Case,	ServeType,	Flavor,	Regular_Diet,	Flavor_Segment,
 from sku where Retailer is NULL