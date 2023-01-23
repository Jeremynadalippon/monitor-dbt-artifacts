/*
This query returns the following information on all models today:
* total runtime
* average runtime
* number of execution
* last execution

Using filters:
* :project_name
* :env
*/
select
    me.schema
    , me.name
    , me.materialization
    , sum(me.total_node_runtime)::number(30, 2) as total_runtime
	, avg(me.total_node_runtime)::number(30, 2) as avg_runtime
    , count(1) as nb_executions
    , max(me.run_started_at) as last_execution
from
	monitoring.dbt_artifacts.model_executions as me
inner join
	monitoring.dbt_artifacts.invocations as i
    on me.command_invocation_id = i.command_invocation_id
where
	i.project_name = :project_name
    and i.target_name = :env
    and me.run_started_at::date = dateadd('day', 0, sysdate())::date
group by
    me.schema
    , me.name
    , me.materialization
order by
    total_runtime desc
