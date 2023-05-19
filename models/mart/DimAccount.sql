{{ config(database= 'dbt',schema='Inventory') }}

with source as (
    select * from {{ ref('EMP') }}
),

employee as (
	select * from {{ source('inventory','employee') }}
),

person_phone as (
    select * from {{ ref('stg_persondetails') }}
),

department as (
	select * from {{ source('inventory','department') }}
),

Employeedepartment as (
	select * from {{ source('inventory','employeedepartment') }}
),

employeedetails as (
    select a.*,b.TotalTaxPayable,b.Salary,b.MedicalInsurance,c.ORGANIZATIONLEVEL,c.JOBTITLE,c.BIRTHDATE,c.MARITALSTATUS,c.GENDER
    c.HIREDATE,c.SALARIEDFLAG,c.VACATIONHOURS,c.CURRENTFLAG,c.MODIFIEDDATE from stg_persondetails a
    join source b on a.BUSINESSENTITYID = b.BUSINESSENTITYID
    left join employee c on a.BUSINESSENTITYID = c.BUSINESSENTITYID
) ,

department as (
    select a.* ,b.NAME from employeedepartment a
    join employee b on a.BUSINESSENTITYID = b.BUSINESSENTITYID
),

final as (
     select {{ dbt_utils.generate_surrogate_key(['a.BUSINESSENTITYID']) }} as employee_key,a.* ,b.NAME,b.startdate,b.enddate from employeedetails a 
     left join department b a.BUSINESSENTITYID = b.BUSINESSENTITYID
)

select * from final





