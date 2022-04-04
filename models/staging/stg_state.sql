with  source as (
	select
	*
	from {{ ref('state') }}
),
stage_state as (
	select
	state_id,
	state_name,
	state_code
	from source
)
select
	*
from stage_state
