{{ config(schema='STAGING') }}

with person as (
	select * from {{ source('inventory','person') }}
),

phone as (

    select * from {{ source('inventory','personphone') }}

),

person_phone as (

    select
        a.* ,b.PHONENUMBER
    
    from person a
	left join phone b 
	on a.BUSINESSENTITYID = b.BUSINESSENTITYID
	
)

select * from person_phone