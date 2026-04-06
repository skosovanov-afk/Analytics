create or replace view public._cron_jobs_diag as
select jobid, jobname, schedule, active, command from cron.job order by jobname;
grant select on public._cron_jobs_diag to service_role;
