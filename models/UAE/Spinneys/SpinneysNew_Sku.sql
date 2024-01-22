{{ config(materialized='table') }}

with Spinneys_ing as (select distinct RetailerName as srcRetailerName ,CAST(srcBarcode as STRING) as Barcode, Material_Code,Article_Code from {{ ref('Spinneys_ing') }} )
,
sku as (
select * from Spinneys_ing SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku_UAE2 SKU
ON SELLOUT.Barcode = SKU.srcBarcode
and SELLOUT.srcRetailerName=SKU.RetailerName
)



select srcRetailerName	,Barcode, Material_Code	,Article_Code,SKU_Name	,Category	,Manufacturer	,Brands,Sub_Brands,Pack_Type,	Pack_Size,	Pack_Size_Range	,Pack_quantity	,Raw_Case,	ServeType	
,Flavor_Segment,Flavor,	Regular_Diet from sku where SKU.RetailerName is NULL