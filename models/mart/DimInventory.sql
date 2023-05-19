{{ config(schema='Inventory') }}

with PRODUCT as (
    select * from {{ source('inventory','product') }}
),
PRODUCTLISTPRICEHISTORY as (
    select * from {{ source('inventory','productcategory')}}
),
PRODUCTSUBCATEGORY as (
    select * from {{ source('inventory','productlistpricehistory')}}
),
PRODUCTCATEGORY as (
    select * from {{ source('inventory','productsubcategory')}}
),
Inventory as (
    SELECT p.productid,(p.PRODUCTNUMBER) AS PRODUCTCODE,p.NAME,pc.NAME AS Category, ps."NAME" AS Subcategory,p.WEIGHTUNITMEASURECODE,p.SIZEUNITMEASURECODE,p.STANDARDCOST,p.FINISHEDGOODSFLAG 
    ,p.color,p.safetystocklevel,p.reorderpoint,ph.listprice,p.SIZE,p.weight,p.daystomanufacture,p.productline,(ph.listprice*0.6) AS DealerPrice,
    p.class,p.STYLE,ph.startdate,ph.enddate,p.WARRANTY,(p.FORMULA) AS BUSINESSSECRET
    FROM PRODUCT p
    JOIN PRODUCTLISTPRICEHISTORY ph ON p.productid=ph.productid
    JOIN PRODUCTSUBCATEGORY  ps ON (p.PRODUCTSUBCATEGORYID=ps.PRODUCTSUBCATEGORYID)
    JOIN PRODUCTCATEGORY  pc ON (ps.PRODUCTCATEGORYID=pc.PRODUCTCATEGORYID)
),
final(
    select {{ dbt_utils.generate_surrogate_key(['a.productid']) }} as inventory_key, a.*
    from Inventory a
)
select * from final