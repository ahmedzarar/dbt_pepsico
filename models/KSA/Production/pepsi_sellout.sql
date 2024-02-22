
{{ config(materialized='incremental', schema='pepsi_sandbox') }}


select * from {{ ref('pepsi_sellout_production') }} 

{% if is_incremental() %}

 -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
where RetailerName='PANDA' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_sellout Where RetailerName='PANDA')

{% endif %}

union all

select * from {{ ref('pepsi_sellout_production') }}

{% if is_incremental() %}

 -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
where RetailerName='TAMIMI' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_sellout Where RetailerName='TAMIMI')

{% endif %}

union all

select * from {{ ref('pepsi_sellout_production') }}

{% if is_incremental() %}

 -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
where RetailerName='OTHAIM' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_sellout Where RetailerName='OTHAIM')

{% endif %}
union all

select * from {{ ref('pepsi_sellout_production') }}

{% if is_incremental() %}

 -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
where RetailerName IN ('BINDAWOOD','DANUBE') AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_sellout Where RetailerName IN ('BINDAWOOD','DANUBE'))

{% endif %}