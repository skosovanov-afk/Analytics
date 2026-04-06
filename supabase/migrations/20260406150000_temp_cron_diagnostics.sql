-- Temporary diagnostic view to check cron jobs via PostgREST
create or replace view public._cron_jobs_diag as
select jobid, jobname, schedule, active, command
from cron.job
order by jobname;

grant select on public._cron_jobs_diag to anon, authenticated, service_role;

-- Also check last HTTP responses from pg_net
create or replace view public._net_responses_diag as
select id, status_code, timed_out,
       left(content::text, 300) as content_preview,
       created
from net._http_response
order by id desc
limit 20;

grant select on public._net_responses_diag to anon, authenticated, service_role;
