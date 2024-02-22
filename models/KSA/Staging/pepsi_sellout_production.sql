
{{ config(materialized='table', schema='pepsi_sandbox') }}
With Cte as(
select *, CASE
        WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (325.0) AND Manufacturer_new IN ('PEP') AND Pack_Quantity IN (1,6,24)  THEN 'PROMO'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (325.0) AND Manufacturer_new IN ('KO') AND Pack_Quantity IN (1,4,12,24)  THEN 'PROMO'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'MS PET' AND SKU_Volume IN (2200.0) AND Manufacturer_new IN ('PEP','KO') AND Pack_Quantity IN (1,6) THEN 'PROMO'
        WHEN category_definition IN ('CSD') AND SKU_Volume IN (330.0) AND Manufacturer_new IN ('PEP','KO') AND Pack_Quantity IN (18)  THEN 'PROMO'
        WHEN category_definition IN ('CSD') AND PromoType_Rev_MGT IS NULL THEN 'NON-PROMO'
    END as PromoType_Rev_MGT_1,
CASE
    WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (200.0, 222.0, 240.0, 250.0, 252.0, 275.0, 300.0, 320.0, 325.0, 330.0, 335.0, 350.0, 355.0, 360.0, 500.0) THEN 24
    WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (150.0, 185.0) THEN 30
    WHEN category_definition IN ('CSD') AND Pack_Type = 'NRB' AND SKU_Volume IN (200.0, 250.0, 275.0, 295.0, 300.0, 330.0, 355.0, 375.0, 400.0, 290.0, 290, 500.0, 500, 750.0) THEN 24
    WHEN category_definition IN ('CSD') AND Pack_Type = 'SS PET' AND SKU_Volume IN (200.0, 330.0, 400.0, 500.0, 591.0) THEN 24
    WHEN category_definition IN ('CSD') AND Pack_Type = 'MS PET' AND SKU_Volume IN (1000.0, 1500.0, 1750.0, 2000.0, 2200.0, 2250.0, 2500.0) THEN 6
    WHEN category_definition = 'WATER' AND SKU_Volume IN (125.0, 200.0) THEN 48
    WHEN category_definition = 'WATER' AND SKU_Volume IN (230, 250, 270, 230, 300, 330, 350, 340, 355, 375, 400) THEN 30
    WHEN category_definition = 'WATER' AND SKU_Volume IN (473.0,500, 502.8, 550, 591, 600, 620, 660, 700) THEN 24
    WHEN category_definition = 'WATER' AND SKU_Volume IN (750, 800, 850, 946, 1000, 1250, 1500, 2000, 3800, 5000, 6000, 8000, 10000, 12000, 15000, 17500, 18900, 18927, 19000, 20000) THEN 6
  END as Raw_Case_1,
 CASE
        WHEN category_definition IN ('CSD') AND Pack_Type IN ('CANS','SS PET') AND SKU_Volume IN (150.0,185.0,200.0) THEN '150ML - 200ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'NRB' AND SKU_Volume IN (200.0) THEN '150ML - 200ML'
        WHEN category_definition IN ('CSD') AND Pack_Type IN ('NRB','CANS') AND SKU_Volume IN (222.0,230.0,240.0,250.0) THEN '201ML - 250ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (252.0,275.0,300.0,320.0,325.0,330.0) THEN '251ML - 330ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'NRB' AND SKU_Volume IN (355.0,375.0,400.0) THEN '331ML - 400ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'NRB' AND SKU_Volume IN (750.0) THEN '750ML - 1000ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (335.0,350.0,355.0,360.0) THEN '331ML - 400ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'SS PET' AND SKU_Volume IN (330.0) THEN '251ML - 330ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'SS PET' AND SKU_Volume IN (400.0) THEN '331ML - 400ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'SS PET' AND SKU_Volume IN (500.0,591.0) THEN '401ML - 749ML'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'MS PET' AND SKU_Volume IN (1000.0) THEN '1L - 1.49L'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'MS PET' AND SKU_Volume IN (1500.0,1750.0) THEN '1.5L - 1.99L'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'MS PET' AND SKU_Volume IN (2000.0,2200.0,2250.0,2500.0) THEN '2L+'
        WHEN category_definition IN ('CSD') AND Pack_Type = 'NRB' AND SKU_Volume IN (275.0,275,290,290.0,295.0,295,300.0,300,330) THEN '251ML - 330ML'
        WHEN category_definition IN ('CSD') AND Pack_Type IN ('CANS','NRB') AND SKU_Volume IN (500.0,500) THEN '401ML - 749ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (125.0) THEN '< 200ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (200.0) THEN '200ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (230.0,250.0,270.0,300.0) THEN '201ML - 329ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (330.0) THEN '330ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (375.0,350.0,355.0,340.0,400.0,473.0) THEN '331ML - 499ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (500.0) THEN '500ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (502.8,550.0,591.0,600.0,620.0,660.0,700.0,750.0) THEN '501ML - 750ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (800.0,850.0,946.0) THEN '751ML - 999ML'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (1000.0) THEN '1L'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (1250.0) THEN '1L - 1.49L'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (1500.0) THEN '1.5L'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (2000.0,3800.0) THEN '1.6L - 4.9L'
        WHEN category_definition IN ('WATER') AND SKU_Volume IN (5000,6000,8000,10000,12000,15000,17500,18900,18927,19000,20000) THEN '5L+'
    END as Volume_category_1,
  CASE
    WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (330.0, 355.0, 360.0, 320.0, 350.0, 335.0) AND Pack_Quantity NOT IN (18)
      THEN CONCAT('320ML-360ML x ', CAST(Pack_Quantity AS STRING))
    WHEN category_definition IN ('CSD') AND Pack_Type = 'CANS' AND SKU_Volume IN (355.0, 360.0, 320.0, 350.0, 335.0) AND Pack_Quantity IN (18)
      THEN CONCAT('320ML-360ML x ', CAST(Pack_Quantity AS STRING))
    WHEN category_definition IN ('CSD')
      THEN CONCAT(CAST(SKU_Volume AS STRING), 'ML x ', COALESCE(CAST(Pack_Quantity AS STRING), ''))
    WHEN category_definition IN ('WATER')
      THEN CONCAT(CAST(SKU_Volume AS STRING), 'ML x ', COALESCE(CAST(Pack_Quantity AS STRING), ''))
    ELSE Pricing_Vol_Category
  END as Pricing_Vol_Category_1,

CASE
    WHEN (Promo_Code IS NULL or Promo_Code='') THEN 'UNSPECIFIED'
    WHEN Promo_Code = 'R' THEN 'NON-PROMO'
    WHEN PromoType_Retailer IS NULL THEN 'PROMO'
    ELSE PromoType_Retailer
END as PromoType_Retailer_1,
CONCAT(COALESCE(Sub_Brand,"")," ",COALESCE(Pack_Type,"")," ",COALESCE(cast(Pack_Quantity as STRING),""),COALESCE("X"),COALESCE(cast(SKU_Volume as STRING),"")) as SKU_ID_1 


 from {{ ref('pepsi_sellout_Staging') }})

 select  RetailerName ,  Year ,  Month ,  Channel ,  StoreCode ,  StoreName ,  Country ,  Bottler ,  City ,  SKUName ,  Sale_Qty ,  Sale_Price ,  Manufacturer_new ,  Manufacturer_ShortName ,  Promo_Type ,  Volume_category_1 as Volume_category ,  Total_volume_Grams ,  Brand_Name ,  Family_Name ,  Pack_Type ,  category_definition ,  Volume_type ,  Serve_Type ,  Pack_Quantity ,  SKU_Volume ,  Bundle_Type ,  Total_volume ,  Sub_Brand ,  Unit_Price ,  Key ,  Raw_Case_1 as Raw_Case,  ServeType ,  Flavor ,  Regular_Diet ,  Promo_Code ,  PromoType_Rev_MGT_1 as PromoType_Rev_MGT  ,  PromoType_Retailer_1 as PromoType_Retailer ,  Pricing_Vol_Category_1 as Pricing_Vol_Category ,  SKU_Code ,  srcStoreCode ,  srcStoreName ,  srcSKUCode ,  srcSKUName ,  Flavor_Segment ,  srcPromo_Week ,  Sale_Price_withVAT ,  Jan_2019_till_Jun_2020 ,  July_2020_till_Dec_2021 ,  Promo_Flagging , SKU_ID_1 as SKU_ID 
 from Cte