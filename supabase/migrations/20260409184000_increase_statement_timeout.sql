-- Increase statement timeout for service_role to allow SmartLead sync
-- Default Supabase timeout is 8s which is too short for full table scans
ALTER ROLE service_role SET statement_timeout = '60s';
