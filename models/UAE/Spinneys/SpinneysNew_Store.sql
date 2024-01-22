{{
    config(
        materialized='table',
        incremental_strategy = 'insert_overwrite'
    )
}}

with Spinneys_ing as (select distinct RetailerName as Retailer,Cast(srcStore as STRING) as Store_Code  from {{ ref('Spinneys_ing') }} )
,
sku as (
select * from Spinneys_ing SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store_UAE Store
ON SELLOUT.Store_Code = Store.srcStore
and SELLOUT.Retailer=Store.RetailerName)


select Retailer,Store_Code from sku where srcStore is NULL