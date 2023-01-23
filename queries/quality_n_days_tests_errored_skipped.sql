/*
This query returns test's errors and/or skipped and their start in the last n days.

Using filters:
* :project_name
* :env
* :daterange
*/
select
    te.node_id
    , te.status
    , te.run_started_at
    , te.total_node_runtime
from
	monitoring.dbt_artifacts.test_executions as te
inner join
	monitoring.dbt_artifacts.invocations as i
    on te.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and te.run_started_at::date = :daterange
    and te.status in ('error', 'warn', 'skipped')
;

select
top 10 *
from
	monitoring.dbt_artifacts.test_executions as te
inner join
	monitoring.dbt_artifacts.invocations as i
    on te.command_invocation_id = i.command_invocation_id
where
    te.status in ('error', 'warn', 'skipped') and failures is not null