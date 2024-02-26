{{ config(materialized='table', schema='pepsi_sandbox') }}

SELECT SELLOUT.RetailerName, SELLOUT.Year, SELLOUT.Month, IF(SELLOUT.RetailerName = 'CARREFOUR', SELLOUT.srcChannel, MARKET.Channel) AS Channel, MARKET.StoreCode, MARKET.StoreName, MARKET.Country, MARKET.Bottler, MARKET.City, SKU.SKU_NAME as SKUName , SELLOUT.Sale_Qty, SELLOUT.Sale_Price, SKU.Manufacturer as Manufacturer_new,Cast (Null as String) As Manufacturer_ShortName ,Cast(Null as String) as Promo_Type, SKU.Pack_Size_Range as Volume_category,Cast(Null as Float64) as Total_volume_Grams, SKU.Brands as Brand_Name,Cast (Null as String)  as Family_Name, SKU.Pack_Type,SKU.Category as category_definition ,Cast (Null as String)  as Volume_type,Cast (Null as String)  as  Serve_Type, SKU.Pack_quantity as Pack_Quantity , CAST(SKU.Pack_Size as FLOAT64) AS SKU_Volume,Cast (Null as String) as Bundle_Type,Cast(Null as Float64) as Total_volume, SKU.Sub_Brands as Sub_Brand,Cast(Null as Float64) as Unit_Price,Cast(Null as String) as Key, SKU.Raw_Case, SKU.ServeType, SKU.Flavor, SKU.Regular_Diet, SELLOUT.srcPromoOfferCode as Promo_Code,
Cast(Null as String) as  PromoType_Rev_MGT , Cast( Null as string) as PromoType_Retailer, Cast( Null as string) as Pricing_Vol_Category,
SKU.SKU_CODE, SELLOUT.srcStore as srcStoreCode , SELLOUT.srcStoreName , SELLOUT.SKU_Code as srcSKUCode, SELLOUT.srcSKU as srcSKUName, SKU.Flavor_Segment, SELLOUT.srcPromo_Week, SELLOUT.Sale_Price_withVAT, Cast(Null as Float64) as  Jan_2019_till_Jun_2020,
Cast(Null as Float64) as  July_2020_till_Dec_2021, Cast(Null as String) as Promo_Flagging,Cast(Null as String) as SKU_ID
 FROM
(SELECT RetailerName, Year, Month, srcStore, srcStoreName, srcChannel,
IF(RetailerName = 'CARREFOUR', srcBarcode, srcSKU_Code) AS SKU_Code, srcSKU,
Sale_Qty,Sale_Price,srcPromoOfferCode,srcPromo_Week,IF(RetailerName = 'PANDA', Sale_Price, Sale_Price+Sale_Price*.15) as Sale_Price_withVAT FROM {{ ref('Panda_ing') }} ) SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store MARKET
ON SELLOUT.srcStore = MARKET.srcStore
AND SELLOUT.RetailerName = MARKET.RetailerName
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku SKU
ON SELLOUT.SKU_Code = SKU.SKU_CODE
AND SELLOUT.RetailerName = SKU.Retailer
WHERE SKU.Category IN ('CSD','WATER')

UNION ALL

SELECT SELLOUT.RetailerName, SELLOUT.Year, SELLOUT.Month, IF(SELLOUT.RetailerName = 'CARREFOUR', SELLOUT.srcChannel, MARKET.Channel) AS Channel, MARKET.StoreCode, MARKET.StoreName, MARKET.Country, MARKET.Bottler, MARKET.City, SKU.SKU_NAME as SKUName ,
CASE 
    WHEN SKU.Category = 'CSD' AND SELLOUT.RetailerName = 'OTHAIM' AND SKU.SKU_NAME IN ('MOUNTAIN DEW CAN 18x355ML', 'MIRINDA ORANGE CAN 18x355ML', 'PEPSI CAN 18x330ML', '7 UP CAN 18x330ML', '7 UP DIET/FREE CAN 18x355ML', 'PEPSI DIET CAN 18x355ML', 'MIRINDA CITRUS CAN 18x330ML') THEN SELLOUT.Sale_Qty * 18
    WHEN SKU.Category = 'WATER' AND SELLOUT.RetailerName = 'OTHAIM' AND SKU.SKU_NAME IN ('ERTWAINA WATER 20x330ML','ERTWAINA WATER 20x200ML','HALEY HEALTHY WATER 20x330ML') THEN SELLOUT.Sale_Qty * 20
    WHEN SKU.Category = 'WATER' AND SELLOUT.RetailerName = 'OTHAIM' AND SKU.SKU_NAME IN ('HALEY WATER 24x200ML','HALAY WATER 24X200') THEN SELLOUT.Sale_Qty * 24
ELSE SELLOUT.Sale_Qty
END as Sale_Qty,
SELLOUT.Sale_Price, SKU.Manufacturer as Manufacturer_new,Cast (Null as String) As Manufacturer_ShortName ,Cast(Null as String) as Promo_Type, SKU.Pack_Size_Range as Volume_category,Cast(Null as Float64) as Total_volume_Grams, SKU.Brands as Brand_Name,Cast (Null as String)  as Family_Name, SKU.Pack_Type,SKU.Category as category_definition ,Cast (Null as String)  as Volume_type,Cast (Null as String)  as  Serve_Type, SKU.Pack_quantity as Pack_Quantity , CAST(SKU.Pack_Size as FLOAT64) AS SKU_Volume,Cast (Null as String) as Bundle_Type,Cast(Null as Float64) as Total_volume, SKU.Sub_Brands as Sub_Brand,Cast(Null as Float64) as Unit_Price,Cast(Null as String) as Key, SKU.Raw_Case, SKU.ServeType, SKU.Flavor, SKU.Regular_Diet, SELLOUT.srcPromoOfferCode as Promo_Code,
Cast(Null as String) as  PromoType_Rev_MGT , Cast( Null as string) as PromoType_Retailer,  Cast( Null as string) as Pricing_Vol_Category,
SKU.SKU_CODE, SELLOUT.srcStore as srcStoreCode , SELLOUT.srcStoreName , SELLOUT.SKU_Code as srcSKUCode, SELLOUT.srcSKU as srcSKUName, SKU.Flavor_Segment, SELLOUT.srcPromo_Week, SELLOUT.Sale_Price_withVAT, Cast(Null as Float64) as  Jan_2019_till_Jun_2020,
Cast(Null as Float64) as  July_2020_till_Dec_2021, Cast(Null as String) as Promo_Flagging,Cast(Null as String) as SKU_ID
 FROM
(SELECT RetailerName, Year, Month, srcStore, srcStoreName, srcChannel,
IF(RetailerName = 'CARREFOUR', srcBarcode, srcSKU_Code) AS SKU_Code, srcSKU,
Sale_Qty,Sale_Price,srcPromoOfferCode,srcPromo_Week,IF(RetailerName = 'PANDA', Sale_Price, Sale_Price+Sale_Price*.15) as Sale_Price_withVAT FROM {{ ref('Othaim_ing') }} ) SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store MARKET
ON SELLOUT.srcStore = MARKET.srcStore
AND SELLOUT.RetailerName = MARKET.RetailerName
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku SKU
ON SELLOUT.SKU_Code = SKU.SKU_CODE
AND SELLOUT.RetailerName = SKU.Retailer
WHERE SKU.Category IN ('CSD','WATER')

UNION ALL

SELECT SELLOUT.RetailerName, SELLOUT.Year, SELLOUT.Month, IF(SELLOUT.RetailerName = 'CARREFOUR', SELLOUT.srcChannel, MARKET.Channel) AS Channel, MARKET.StoreCode, MARKET.StoreName, MARKET.Country, MARKET.Bottler, MARKET.City, SKU.SKU_NAME as SKUName , SELLOUT.Sale_Qty, SELLOUT.Sale_Price, SKU.Manufacturer as Manufacturer_new,Cast (Null as String) As Manufacturer_ShortName ,Cast(Null as String) as Promo_Type, SKU.Pack_Size_Range as Volume_category,Cast(Null as Float64) as Total_volume_Grams, SKU.Brands as Brand_Name,Cast (Null as String)  as Family_Name, SKU.Pack_Type,SKU.Category as category_definition ,Cast (Null as String)  as Volume_type,Cast (Null as String)  as  Serve_Type, SKU.Pack_quantity as Pack_Quantity , CAST(SKU.Pack_Size as FLOAT64) AS SKU_Volume,Cast (Null as String) as Bundle_Type,Cast(Null as Float64) as Total_volume, SKU.Sub_Brands as Sub_Brand,Cast(Null as Float64) as Unit_Price,Cast(Null as String) as Key, SKU.Raw_Case, SKU.ServeType, SKU.Flavor, SKU.Regular_Diet, SELLOUT.srcPromoOfferCode as Promo_Code,
Cast(Null as String) as  PromoType_Rev_MGT ,Cast( Null as string) as PromoType_Retailer,  Cast( Null as string) as Pricing_Vol_Category,
SKU.SKU_CODE, SELLOUT.srcStore as srcStoreCode , SELLOUT.srcStoreName , SELLOUT.SKU_Code as srcSKUCode, SELLOUT.srcSKU as srcSKUName, SKU.Flavor_Segment, SELLOUT.srcPromo_Week, SELLOUT.Sale_Price_withVAT, Cast(Null as Float64) as  Jan_2019_till_Jun_2020,
Cast(Null as Float64) as  July_2020_till_Dec_2021 ,Cast(Null as String) as Promo_Flagging,Cast(Null as String) as SKU_ID
 FROM
(SELECT RetailerName, Year, Month, srcStore, srcStoreName, srcChannel,
IF(RetailerName = 'CARREFOUR', srcBarcode, srcSKU_Code) AS SKU_Code, srcSKU,
Sale_Qty,Sale_Price,srcPromoOfferCode,srcPromo_Week,IF(RetailerName = 'PANDA', Sale_Price, Sale_Price+Sale_Price*.15) as Sale_Price_withVAT FROM {{ ref('BND_ing') }} ) SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store MARKET
ON SELLOUT.srcStore = MARKET.srcStore
AND SELLOUT.RetailerName = MARKET.RetailerName
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku SKU
ON SELLOUT.SKU_Code = SKU.SKU_CODE
AND SELLOUT.RetailerName = SKU.Retailer
WHERE SKU.Category IN ('CSD','WATER')

UNION ALL

SELECT SELLOUT.RetailerName, SELLOUT.Year, SELLOUT.Month, IF(SELLOUT.RetailerName = 'CARREFOUR', SELLOUT.srcChannel, MARKET.Channel) AS Channel, MARKET.StoreCode, MARKET.StoreName, MARKET.Country, MARKET.Bottler, MARKET.City, SKU.SKU_NAME as SKUName , SELLOUT.Sale_Qty, SELLOUT.Sale_Price, SKU.Manufacturer as Manufacturer_new,Cast (Null as String) As Manufacturer_ShortName ,Cast(Null as String) as Promo_Type, SKU.Pack_Size_Range as Volume_category,Cast(Null as Float64) as Total_volume_Grams, SKU.Brands as Brand_Name,Cast (Null as String)  as Family_Name, SKU.Pack_Type,SKU.Category as category_definition ,Cast (Null as String)  as Volume_type,Cast (Null as String)  as  Serve_Type, SKU.Pack_quantity as Pack_Quantity , CAST(SKU.Pack_Size as FLOAT64) AS SKU_Volume,Cast (Null as String) as Bundle_Type,Cast(Null as Float64) as Total_volume, SKU.Sub_Brands as Sub_Brand,Cast(Null as Float64) as Unit_Price,Cast(Null as String) as Key, SKU.Raw_Case, SKU.ServeType, SKU.Flavor, SKU.Regular_Diet, SELLOUT.srcPromoOfferCode as Promo_Code,
Cast(Null as String) as  PromoType_Rev_MGT , Cast( Null as string) as PromoType_Retailer, Cast( Null as string) as Pricing_Vol_Category,
SKU.SKU_CODE, SELLOUT.srcStore as srcStoreCode , SELLOUT.srcStoreName , SELLOUT.SKU_Code as srcSKUCode, SELLOUT.srcSKU as srcSKUName, SKU.Flavor_Segment, SELLOUT.srcPromo_Week, SELLOUT.Sale_Price_withVAT, Cast(Null as Float64) as  Jan_2019_till_Jun_2020,
Cast(Null as Float64) as  July_2020_till_Dec_2021, Cast(Null as String) as Promo_Flagging,Cast(Null as String) as SKU_ID
 FROM
(SELECT RetailerName, Year, Month, srcStore, srcStoreName, srcChannel,
IF(RetailerName = 'CARREFOUR', srcBarcode, srcSKU_Code) AS SKU_Code, srcSKU,
Sale_Qty,Sale_Price,srcPromoOfferCode,srcPromo_Week,IF(RetailerName = 'PANDA', Sale_Price, Sale_Price+Sale_Price*.15) as Sale_Price_withVAT FROM {{ ref('Tamimi_ing') }} ) SELLOUT
LEFT OUTER JOIN pepsi_sandbox.pepsi_store MARKET
ON SELLOUT.srcStore = MARKET.srcStore
AND SELLOUT.RetailerName = MARKET.RetailerName
LEFT OUTER JOIN pepsi_sandbox.pepsi_sku SKU
ON SELLOUT.SKU_Code = SKU.SKU_CODE
AND SELLOUT.RetailerName = SKU.Retailer
WHERE SKU.Category IN ('CSD','WATER')