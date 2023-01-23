/*
This query returns model's runtime per hour in the last n days.
It allows detection of anormal long runtime, periodicity and overall
runtime augmentation.

Using filters:
* :project_name
* :env
*/

select
    me.name
    , date_trunc(hour, me.run_started_at) as run_hour
    , sum(me.total_node_runtime)::number(30, 2) as runtime
   
from
	monitoring.dbt_artifacts.model_executions as me
inner join
	monitoring.dbt_artifacts.invocations as i
    on me.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and me.run_started_at::date = :daterange
group by
    1, 2
