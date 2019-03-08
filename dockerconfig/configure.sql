ALTER USER "HR" IDENTIFIED BY "hr" DEFAULT TABLESPACE "USERS" TEMPORARY TABLESPACE "TEMP"ACCOUNT UNLOCK;
ALTER USER "HR" QUOTA UNLIMITED ON USERS;
ALTER USER "HR" DEFAULT ROLE "CONNECT","RESOURCE";

CREATE TABLESPACE tbs_perm_01
  DATAFILE 'tbs_perm_01.dat' 
    SIZE 250M
  ONLINE;

CREATE TEMPORARY TABLESPACE tbs_temp_01
  TEMPFILE 'tbs_temp_01.dbf'
    SIZE 5M
    AUTOEXTEND ON;

CREATE USER unichem
  IDENTIFIED BY unichem
  DEFAULT TABLESPACE tbs_perm_01
  TEMPORARY TABLESPACE tbs_temp_01
  QUOTA 200M on tbs_perm_01;

GRANT create session TO unichem;
GRANT create table TO unichem;
GRANT create view TO unichem;
GRANT create any trigger TO unichem;
GRANT create any procedure TO unichem;
GRANT create sequence TO unichem;
GRANT create synonym TO unichem;
GRANT connect to unichem;
GRANT ALL PRIVILEGES TO unichem;
