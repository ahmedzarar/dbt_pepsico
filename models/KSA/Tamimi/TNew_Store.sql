{{
    config(
        materialized='table',
        incremental_strategy = 'insert_overwrite'
    )
}}


with tamimi_ing as (select distinct srcStore as srcStore_Num,RetailerName as srcRetailerName  from {{ ref('Tamimi_ing') }})
,
sku as (
select * from tamimi_ing SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store Store
ON SELLOUT.srcStore_Num = Store.srcStore
and SELLOUT.srcRetailerName=Store.RetailerName)


select srcStore_Num,srcRetailerName from sku where srcStore is NULL