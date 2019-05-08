set colsep "|"
set echo off
set termout off
set underline off
set heading off
set feedback off
set linesize 32767
set trimspool on
set trimout on
set verify off
set newpage 0
set pagesize 0
spool /opt/unichem/csv_file2M.csv
SELECT uc.UCI || '|' || uc.STANDARDINCHI || '|' || uc.STANDARDINCHIKEY || '|' || pa.PARENT_SMILES FROM UC_INCHI uc, SS_PARENTS pa WHERE uc.UCI < 1000000 AND uc.UCI = pa.UCI;
spool off
