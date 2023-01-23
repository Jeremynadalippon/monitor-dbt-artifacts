/*
This query returns total runtime for tests today
and yesterday at the same hour/minute for comparison.

Using filters:
* :project_name
* :env
*/
select
    sum(case when te.run_started_at::date = sysdate()::date then te.total_node_runtime else 0 end)::number as today
    , sum(case when te.run_started_at::date = dateadd('day', -1, sysdate())::date
        and hour(te.run_started_at) <= hour(sysdate()) and not ( te.run_started_at::date = dateadd('day', -1, sysdate())::date
        and hour(te.run_started_at) = hour(sysdate()) and minute(te.run_started_at) >= minute(sysdate()) ) then te.total_node_runtime else 0 end)::number as yesterday
from
	monitoring.dbt_artifacts.test_executions as te
inner join
	monitoring.dbt_artifacts.invocations as i
    on te.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and te.run_started_at::date >= dateadd('day', -1, sysdate())::date
