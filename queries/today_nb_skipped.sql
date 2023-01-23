/*
This query returns nb models skipped today
and yesterday at the same hour/minute for comparison.

Using filters:
* :project_name
* :env
*/
select
	coalesce(sum(case when me.run_started_at::date = sysdate()::date then 1 else 0 end), 0) as today
    , coalesce(sum(case when me.run_started_at::date = dateadd('day', -1, sysdate())::date
        and hour(me.run_started_at) <= hour(sysdate())and not ( me.run_started_at::date = dateadd('day', -1, sysdate())::date
        and hour(me.run_started_at) = hour(sysdate()) and minute(me.run_started_at) >= minute(sysdate()) ) then 1 else 0 end)::number, 0) as yesterday
from
	monitoring.dbt_artifacts.model_executions as me
inner join
	monitoring.dbt_artifacts.invocations as i
    on me.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and me.run_started_at::date >= dateadd('day', -1, sysdate())::date
    and me.status = 'skipped'
