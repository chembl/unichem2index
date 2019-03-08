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
SELECT UCI || '|' || STANDARDINCHI || '|' || STANDARDINCHIKEY || '|' || SRC_COMPOUND_ID || '|' || NAME FROM (select str.UCI, str.STANDARDINCHI, str.STANDARDINCHIKEY, xrf.SRC_COMPOUND_ID, so.NAME FROM UC_STRUCTURE str, UC_XREF xrf, UC_SOURCE so WHERE xrf.UCI = str.UCI AND so.SRC_ID = xrf.SRC_ID ORDER BY UCI) WHERE ROWNUM <= 1000000;
spool off
