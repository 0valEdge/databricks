{{ config(schema='STAGING') }}

with stateprovince as (
    select * from {{ source('inventory','stateprovince') }}
),

address as (
    select * from {{ source('inventory','address') }}
),

stateprovince_address as (
    select
         a.*,b.name,b.countryregioncode,b.stateprovincecode
        from address a
        inner join stateprovince b on a.stateprovinceid = b.stateprovinceid 
        )

select
	*
from stateprovince_address
