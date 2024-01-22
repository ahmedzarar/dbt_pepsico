{{
    config(
        materialized='table',
        incremental_strategy = 'insert_overwrite'
    )
}}

with Othaim_ing as (select distinct srcStore as srcStore_Num,RetailerName as srcRetailerName  from {{ ref('Othaim_ing') }} )
,
sku as (
select * from Othaim_ing SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store Store
ON SELLOUT.srcStore_Num = Store.StoreCode
and SELLOUT.srcRetailerName=Store.RetailerName)


select srcStore_Num,srcRetailerName from sku where StoreName is NULL