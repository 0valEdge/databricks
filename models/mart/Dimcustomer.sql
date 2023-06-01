{{ config(schema='Inventory') }}

with stg_customer as (
    select * from {{ ref('stg_customer') }}
),

person_phone as (
    select * from {{ ref('stg_persondetails') }}
),
stateprovince_address as (
    select * from {{ ref('stg_Address') }}
),
	 
Persondetails as (

    select 
        a.*,b.STATEPROVINCEID,b.NAME,b.ADDRESSLINE1,b.ADDRESSLINE2,b.CITY,b.POSTALCODE,b.COUNTRYREGIONCODE
    from person_phone a
    left join stateprovince_address b
	on a.BUSINESSENTITYID = b.BUSINESSENTITYID
    
),

final as (

select {{ dbt_utils.generate_surrogate_key(['a.BUSINESSENTITYID']) }} as customer_key,a.*,
b.zipcode
	 from Persondetails a
		inner join stg_customer b on a.BUSINESSENTITYID = b.customer_id
)
select * from final