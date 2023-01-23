/*
This query returns model's errors and their start in the last n days.

Using filters:
* :project_name
* :env
* :daterange
*/
select
    me.name
    , me.materialization
    , me.status
    , me.run_started_at
from
	monitoring.dbt_artifacts.model_executions as me
inner join
	monitoring.dbt_artifacts.invocations as i
    on me.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and me.run_started_at::date = :daterange
    and me.status in ('error', 'skipped')
;
