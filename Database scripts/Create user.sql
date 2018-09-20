-- User: jmssantos
-- DROP USER jmssantos;

CREATE USER jmssantos WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;

GRANT pg_stat_scan_tables TO jmssantos WITH ADMIN OPTION;