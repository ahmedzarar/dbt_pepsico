{{ config(materialized='incremental', schema='pepsi_sandbox') }}

select * from {{ ref('Panda_ing') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
  where RetailerName='PANDA' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_ingestion Where RetailerName='PANDA')

{% endif %}

UNION ALL

select * from {{ ref('Othaim_ing') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
  where RetailerName='OTHAIM' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_ingestion Where RetailerName='OTHAIM')

{% endif %}

UNION ALL

select * from {{ ref('Tamimi_ing') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
  where RetailerName='TAMIMI' AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_ingestion Where RetailerName='TAMIMI')

{% endif %}

UNION ALL 
 select * from {{ ref('BND_ing') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
{{ config(materialized='incremental', schema='pepsi_sandbox') }}
  where RetailerName IN ('BINDAWOOD','DANUBE') AND DATE(Year, 1, Month) > (select max(DATE(Year, 1, Month)) from pepsico-304512.pepsi_sandbox.pepsi_ingestion Where RetailerName IN ('BINDAWOOD','DANUBE'))

{% endif %}
