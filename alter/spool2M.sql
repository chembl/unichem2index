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
SELECT UCI || '|' || STANDARDINCHI || '|' || STANDARDINCHIKEY || '|' || PARENT_SMILES || '|' || SRC_COMPOUND_ID || '|' || src_id || '|' || NAME_LONG || '|' || NAME_LABEL || '|' || DESCRIPTION || '|' || BASE_ID_URL
FROM (
    SELECT uc.UCI,
           uc.STANDARDINCHI,
           uc.STANDARDINCHIKEY,
           pa.PARENT_SMILES,
           xref.SRC_COMPOUND_ID,
           so.src_id,
           so.NAME_LONG,
           so.NAME_LABEL,
           so.DESCRIPTION,
           so.BASE_ID_URL
    FROM UC_INCHI UC,
         SS_PARENTS pa,
         UC_SOURCE@reader1dblink so,
         UC_XREF@reader1dblink xref
    WHERE xref.UCI = uc.UCI
      AND UC.UCI >= 0
      AND UC.UCI < 2000000
      AND uc.UCI = pa.UCI
      AND xref.src_id = so.src_id
    );
spool off
