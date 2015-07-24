------------------------------------------------------------------------------------
-- File name  : spexp.sql
-- Version    : v1.0.12 beta 03-Jun-2013
-- Purpose    : The simple data export tool
-- Author     : Valentin Nikotin (rednikotin@gmail.com)
-- Copyright  : (c) Valentin Nikotin - valentinnikotin.com. All rights reserved.
--
-- Disclaimer of Warranty : 
-- This script is provided "as is" without warranty of any kind or guarantees are
-- made about its correctness, reliability and safety. The entire risk as to the 
-- quality and performance of the script is with you.
--
-- License:
-- You can use this script for free for your personal or commercial purposes.
-- You may modify this script as you like for your personal or commercial purposes,
-- but cannot remove this notice or copyright notices or the banner output by 
-- the program or edit them in any way at all. This header must be the first thing
-- in the beginning of this file. 
-- You also cannot publish or distribute or in anyway make this script available 
-- through any means, instead just link to its location in valentinnikotin.com. 
-- This script cannot be incorporated into any other free or commercial tools 
-- without permission from valentinnikotin.com. You may distribute 
-- this script internally in your company, for internal use only.
------------------------------------------------------------------------------------
--
--
-- Usage:
--   @spexp
--      -from_table, -f              : table or view for export
--      -from_query, -q              : query for export
--      -to_table, -t                : table to which data is exporting, default = EXP_<from_table> or QUERY_<DATE>
--      -where_clause, -w            : optional add filter confition to export
--      -hint_expr, -hint, -h        : optional add hing to query, will be enclosed into /*+ */
--      -drop_before_create, -d      : add drop clause with purge option to export file
--      -no_create, -nc              : don't add create table statement to export file
--      -short, -s                   : various short combinations for export
--      -plus_cols, -c               : export only these cols (comma separated)
--      -minus_cols, -mc             : export all, except these cols (comma separated)
--      -debug_level, -debug         : add degug information to output, on level 3 create/truncate table SPEXP_LOG to logging 
--      -d1, -d2, -d3 ,              : short aliases for debug
--      -no_exoprt_prompt, -np       : not to add information message about every export_prompt_size (1000 by default) inserted rows
--      -no_commiting                : not to commit every commit_block_size rows (1000 by default)
--      -use_plsql_blocks            : use pl/sql blocks of export_plsql_block_size (1000 by default) inserts, in this case cursor sharing doesn't work
--      -export_file_name, -e        : file for export
--      -os                          : OS type : win or unix (default)
--      -estimate, -est              : export nothing, just estimate rows, time and export file size (only for unix)
--      -convert_characterset, -cc   : added convertation from both source database charactersets to destination database chararactersets
--      -use_gzip, -gz               : spool data to fifo and gzip it, doesn't work with estimation or for WIN
--      -from_query_qvar, -qvar      : use it to pass long query by variable qvar instead of by parameter from_query
--
--   short : ((ash|hist_ash)[N(d|h|m|s)][(d|t|f)])|(sql[Nx][(d|t|f)])
--      ses - gv$session
--      ash - alias for gv$active_session_history
--      hash - alias for dba_hist_active_sess_history
--      sql - gv$sql
--      N(d|h|m|s) - optionaly number of days, hours, minutes or seconds that will be used in the where filter
--      d|t|f - predefined name formats for export table name and export filr name, <alias>_<formatted date>, where formatted date:
--      d - YYYYMMDD
--      t - HH24MISS
--      f - YYYYMMDDHH24MISS
--      x - <DBNAME>_YYMMDD_HH24MI
--      Nx - extra-large clob support, where 1000*N is max clob lenght 
--
-- Note:
--    CLOB, NCLOB dataypes are shrinked to max_clob_length (10000 default) characters
--    BLOB dataypes are shrinked to 2000 characters
--    Supported datatypes:
--       VARCHAR2, CHAR
--       NVARCHAR2, NCHAR
--       NUMBER, INT, INTEGER, FLOAT
--       DATE
--       RAW
--       CLOB
--       NCLOB
--       BLOB
--       TIMESTAMP
--       TIMESTAMP WITH TIMEZONE
--       TIMESTAMP WITH LOCAL TIMEZONE
--       BINARY_FLOAT, BINARY_DOUBLE
--       ROWID, UROWID
--       INTERVAL YEAR TO MONTH
--       INTERVAL DAY TO SECOND

set termout on
prompt
prompt *** spexp.sql v1.0 beta - (c) Valentin Nikotin (valentinnikotin.com) ***
prompt
set termout off
store set plusenv.tmp.sql replace
set head off pages 0 echo off tab off verify off trimspool on trimout on null "" long 32767 serveroutput on lin 2498 feedback off arraysize 100 timing off
whenever sqlerror exit rollback

-- internal
def _max_parameters_count=30
def _all_params=""

col tmpfile1 new_value _tmpfile1
col tmpfile2 new_value _tmpfile2
col tblname new_value _tblname
col expfile new_value _expfile
col debug1 new_value _debug1
col debug2 new_value _debug2
col debug3 new_value _debug3
col tstr new_value _tstr
col dbmssql new_value _dbmssql
col dbmslob new_value _dbmslob
col nounix new_value _nounix
col nowin new_value _nowin
col sp_arraysize new_value _sp_arraysize
col sp_long new_value _sp_long
col echo new_value _echo
col tmpfifo new_value _tmpfifo
col use_gzip new_value _use_gzip
col spoolto new_value _spoolto
col ver new_value _ver

col a newline for a2498

var input_params_cur refcursor
var descraw varchar2(4000)
var ddl_cur refcursor
var data_cur refcursor
var row_count number
var t_start varchar2(30)
var t_end varchar2(30)
var f_size number
var from_nls_characterset varchar2(30)
var from_nls_nchar_characterset varchar2(30)
var qvar_copy varchar2(4000)

-- documented
var from_table varchar2(100)
var from_query varchar2(4000)
var to_table varchar2(30)
var where_clause varchar2(1000)
var hint_expr varchar2(1000)
var drop_before_create number
var no_create number
var short varchar2(100)
var pmcols number
var cols varchar2(4000)
var debug_level number
var export_file_name varchar2(100)
var os varchar2(30)
var estimate number
var convert_characterset number
var use_gzip number

-- undocumented
var method_char varchar2(2)
var method_clob varchar2(2)
var method_nchar varchar2(2)
var method_nclob varchar2(2)
var sub varchar2(13)
var max_clob_size number
var piece_size number
var raw_method_piece_size number
var raw_method_min_size number
var compress_min_size number
var no_optimize_clob_tail number
var clob_as_char number
var max_blob_size number
var raw_piece_size number
var raw_compress_min_size number
var no_raw_compress number
var no_temp number
var no_auto_temp number
var no_clob_stmt number
var no_auto_clob_stmt number
var no_compress number
var no_auto_compress number
var no_dbms_sql number
var no_auto_dbms_sql number
var no_dbms_lob number
var no_auto_dbms_lob number
var export_plsql_block_size number
var commit_block_size number
var export_prompt_size number
var estimate_rows_to_check number
var sp_arraysize number
var sp_long number

select 'spexp_tmp1_'||to_char(sysdate, 'YYYYMMDDHH24MISS')||'.sql' tmpfile1,
       'spexp_tmp2_'||to_char(sysdate, 'YYYYMMDDHH24MISS')||'.sql' tmpfile2,
       'spexp_tmpp_'||to_char(sysdate, 'YYYYMMDDHH24MISS')||'.pip' tmpfifo
from dual;
spool &_tmpfile1
select sys_connect_by_path('ol '||level||' new_val '||level||chr(10),'c') a,
       'select 0'||sys_connect_by_path(level,',')||' from dual where 1=0;' a,
       'begin'||chr(10)||'open :input_params_cur for '||sys_connect_by_path('select q''{'||chr(38)||level||'}'' from dual union all','
')||chr(10)||' select '''' a from dual;'||chr(10)||'end;'||chr(10)||'/' a,
       'def _all_params="'||sys_connect_by_path(level,' ')||'"' a
from dual where level=&_max_parameters_count connect by level<=&_max_parameters_count;
spool off
@&_tmpfile1

select substr(value, 1, instr(value, '.') - 1) ver from v$parameter where name = 'compatible';

exec :qvar_copy := :qvar

set termout on
declare
  l_parameter_num pls_integer;
  l_parameter varchar2(30);
  l_value varchar2(32767);
  l_must_get_value boolean := false;
  l_cnt pls_integer;
  procedure err(i_message varchar2) is
  begin
    dbms_output.put_line('Error: '||i_message);
    raise program_error;
  end;
  procedure pdebug(i_level pls_integer, i_message varchar2) is
  begin
    if i_level <= :debug_level then
      dbms_output.put_line(i_message);
    end if;
  end;
  procedure print_variables is
  begin
    dbms_output.put_line('----------------------------------------------');
    dbms_output.put_line('--     documented');
    dbms_output.put_line('--       from_table ['||:from_table||']');
    dbms_output.put_line('--       from_query ['||:from_query||']');
    dbms_output.put_line('--       to_table ['||:to_table||']');
    dbms_output.put_line('--       where_clause ['||:where_clause||']');
    dbms_output.put_line('--       hint_expr ['||:hint_expr||']');
    dbms_output.put_line('--       short ['||:short||']');
    dbms_output.put_line('--       drop_before_create ['||:drop_before_create||']');
    dbms_output.put_line('--       no_create ['||:no_create||']');
    dbms_output.put_line('--       pmcols ['||:pmcols||']');
    dbms_output.put_line('--       cols ['||:cols||']');
    dbms_output.put_line('--       debug_level ['||:debug_level||']');
    dbms_output.put_line('--       export_file_name ['||:export_file_name||']');
    dbms_output.put_line('--       os ['||:os||']');
    dbms_output.put_line('--       estimate ['||:estimate||']');
    dbms_output.put_line('--       convert_characterset ['||:convert_characterset||']');
    dbms_output.put_line('--       use_gzip ['||:use_gzip||']');
    dbms_output.put_line('--     undocumented');
    dbms_output.put_line('--       method_char ['||:method_char||']');
    dbms_output.put_line('--       method_clob ['||:method_clob||']');
    dbms_output.put_line('--       method_nchar ['||:method_nchar||']');
    dbms_output.put_line('--       method_nclob ['||:method_nclob||']');
    dbms_output.put_line('--       sub ['||:sub||']');
    dbms_output.put_line('--       max_clob_size ['||:max_clob_size||']');
    dbms_output.put_line('--       piece_size ['||:piece_size||']');
    dbms_output.put_line('--       raw_method_piece_size ['||:raw_method_piece_size||']');
    dbms_output.put_line('--       raw_method_min_size ['||:raw_method_min_size||']');
    dbms_output.put_line('--       compress_min_size ['||:compress_min_size||']');
    dbms_output.put_line('--       no_optimize_clob_tail ['||:no_optimize_clob_tail||']');
    dbms_output.put_line('--       clob_as_char ['||:clob_as_char||']');
    dbms_output.put_line('--       max_blob_size ['||:max_blob_size||']');
    dbms_output.put_line('--       raw_piece_size ['||:raw_piece_size||']');
    dbms_output.put_line('--       raw_compress_min_size ['||:raw_compress_min_size||']');
    dbms_output.put_line('--       no_raw_compress ['||:no_raw_compress||']');
    dbms_output.put_line('--       no_temp ['||:no_temp||']');
    dbms_output.put_line('--       no_auto_temp ['||:no_auto_temp||']');
    dbms_output.put_line('--       no_clob_stmt ['||:no_clob_stmt||']');
    dbms_output.put_line('--       no_auto_clob_stmt ['||:no_auto_clob_stmt||']');
    dbms_output.put_line('--       no_compress ['||:no_compress||']');
    dbms_output.put_line('--       no_auto_compress ['||:no_auto_compress||']');
    dbms_output.put_line('--       no_dbms_sql ['||:no_dbms_sql||']');
    dbms_output.put_line('--       no_auto_dbms_sql ['||:no_auto_dbms_sql||']');
    dbms_output.put_line('--       no_dbms_lob ['||:no_dbms_lob||']');
    dbms_output.put_line('--       no_auto_dbms_lob ['||:no_auto_dbms_lob||']');
    dbms_output.put_line('--       export_plsql_block_size ['||:export_plsql_block_size||']');
    dbms_output.put_line('--       commit_block_size ['||:commit_block_size||']');
    dbms_output.put_line('--       export_prompt_size ['||:export_prompt_size||']');
    dbms_output.put_line('--       estimate_rows_to_check ['||:estimate_rows_to_check||']');
    dbms_output.put_line('--       from_nls_characterset ['||:from_nls_characterset||']');
    dbms_output.put_line('--       from_nls_nchar_characterset ['||:from_nls_nchar_characterset||']');
    dbms_output.put_line('--       sp_arraysize ['||:sp_arraysize||']');
    dbms_output.put_line('--       sp_long ['||:sp_long||']');
    dbms_output.put_line('----------------------------------------------');
  end;
begin
  pdebug(0, 'Start parameters parsing');

  -- defaults
  -- documented
  :from_table := '';
  :from_query := '';
  :to_table := '';
  :where_clause := '';
  :hint_expr := '';
  :short := '';
  :drop_before_create := 0;
  :no_create := 0;
  :pmcols := 0;
  :cols := '';
  :debug_level := 0;
  :export_file_name := '';
  :os := 'UNIX';
  :estimate := 0;
  :convert_characterset := 0;
  :use_gzip := 0;

  -- undocumented
  :method_char := 'A';
  :method_clob := 'A';
  :method_nchar := 'R';
  :method_nclob := 'R';
  :sub := '';
  :max_clob_size := 10000;
  :piece_size := 500;
  :raw_method_piece_size := 1200;
  :raw_method_min_size := 200;
  :compress_min_size := 300;
  :no_optimize_clob_tail := 0;
  :clob_as_char := 0;
  :max_blob_size := 2000;
  :raw_piece_size := 1240;
  :raw_compress_min_size := 1000;
  :no_raw_compress := 0;
  :no_temp := 0;
  :no_auto_temp := 0;
  :no_clob_stmt := 0;
  :no_auto_clob_stmt := 0;
  :no_compress := 0;
  :no_auto_compress := 0;
  :no_dbms_sql := 0;
  :no_auto_dbms_sql := 0;
  :no_dbms_lob := 0;
  :no_auto_dbms_lob := 0;
  :export_plsql_block_size := 0;
  :commit_block_size := 1000;
  :export_prompt_size := 1000;
  :estimate_rows_to_check := 1000;
  :sp_arraysize := 100;
  :sp_long := 32767;
  
  loop
    fetch :input_params_cur into l_value;
    exit when :input_params_cur%notfound;
    if l_must_get_value then
      case
        when l_parameter in ('-from_table', '-f')            then :from_table := l_value;
        when l_parameter in ('-from_query', '-q')            then :from_query := l_value;
        when l_parameter in ('-to_table', '-t')              then :to_table := l_value;
        when l_parameter in ('-where_clause', '-w')          then :where_clause := l_value;
        when l_parameter in ('-hint_expr', '-hint', '-h')    then :hint_expr := '/*+'||l_value||'*/';
        when l_parameter in ('-short', '-s')                 then :short := l_value;
        when l_parameter in ('-plus_cols', '-pc')            then :pmcols := +1; :cols := ','||upper(l_value);
        when l_parameter in ('-minus_cols', '-mc')           then :pmcols := -1; :cols := ','||upper(l_value);
        when l_parameter in ('-debug_level', '-debug')       then :debug_level := l_value;
        when l_parameter in ('-export_file_name', '-e')      then :export_file_name := l_value;
        when l_parameter in ('-os')                          then :os := upper(l_value);
        when l_parameter in ('-method_char')                 then :method_char := upper(l_value);
        when l_parameter in ('-method_clob')                 then :method_clob := upper(l_value);
        when l_parameter in ('-method_nchar')                then :method_nchar := upper(l_value);
        when l_parameter in ('-method_nclob')                then :method_nclob := upper(l_value);
        when l_parameter in ('-sub')                         then :sub := l_value;
        when l_parameter in ('-max_clob_size')               then :max_clob_size := l_value;
        when l_parameter in ('-piece_size')                  then :piece_size := l_value;
        when l_parameter in ('-raw_method_piece_size')       then :raw_method_piece_size := l_value;
        when l_parameter in ('-raw_method_min_size')         then :raw_method_min_size := l_value;
        when l_parameter in ('-compress_min_size')           then :compress_min_size := l_value;
        when l_parameter in ('-max_blob_size')               then :max_blob_size := l_value;
        when l_parameter in ('-raw_piece_size')              then :raw_piece_size := l_value;
        when l_parameter in ('-raw_compress_min_size')       then :raw_compress_min_size := l_value;
        when l_parameter in ('-export_plsql_block_size')     then :export_plsql_block_size := l_value;
        when l_parameter in ('-commit_block_size')           then :commit_block_size := l_value;
        when l_parameter in ('-export_prompt_size')          then :export_prompt_size := l_value;
        when l_parameter in ('-estimate_rows_to_check')      then :estimate_rows_to_check := l_value;
        when l_parameter in ('-sp_arraysize')                then :sp_arraysize := l_value;
        when l_parameter in ('-sp_long')                     then :sp_long := l_value;
        else
          err('unknown parameter ['||l_parameter||']');
      end case;
      l_must_get_value := false;
    else
      l_parameter := l_value;
      case  
        when l_parameter in ('-drop_before_create', '-d')    then :drop_before_create := 1;
        when l_parameter in ('-no_create', '-nc')            then :no_create := 1;
        when l_parameter in ('-no_exoprt_prompt', '-np')     then :export_prompt_size := 0;
        when l_parameter in ('-no_commiting')                then :commit_block_size := 0;
        when l_parameter in ('-estimate', '-est')            then :estimate := 1;
        when l_parameter in ('-use_plsql_blocks')            then :export_plsql_block_size := 1000;
        when l_parameter in ('-convert_characterset', '-cc') then :convert_characterset := 1;
        when l_parameter in ('-no_optimize_clob_tail')       then :no_optimize_clob_tail := 1;
        when l_parameter in ('-clob_as_char')                then :clob_as_char := 1;
        when l_parameter in ('-no_raw_compress')             then :no_raw_compress := 1;
        when l_parameter in ('-no_temp')                     then :no_temp := 1;
        when l_parameter in ('-no_auto_temp')                then :no_auto_temp := 1;
        when l_parameter in ('-no_auto_clob_stmt')           then :no_auto_clob_stmt := 1;
        when l_parameter in ('-no_compress')                 then :no_compress := 1;
        when l_parameter in ('-no_auto_compress')            then :no_auto_compress := 1;
        when l_parameter in ('-no_dbms_sql')                 then :no_dbms_sql := 1;
        when l_parameter in ('-no_auto_dbms_sql')            then :no_auto_dbms_sql := 1;
        when l_parameter in ('-no_dbms_lob')                 then :no_dbms_lob := 1;
        when l_parameter in ('-no_auto_dbms_lob')            then :no_auto_dbms_lob := 1;
        when l_parameter in ('-d1')                          then :debug_level := 1;
        when l_parameter in ('-d2')                          then :debug_level := 2;
        when l_parameter in ('-d3')                          then :debug_level := 3;
        when l_parameter in ('-use_gzip', '-gz')             then :use_gzip := 1;
        when l_parameter in ('-from_query_qvar', '-qvar')    then :from_query := :qvar_copy;
        when l_parameter is null then
          null;
        else l_must_get_value := true;
      end case;
    end if;
  end loop;
  if l_must_get_value then
    err('unknown parameter ['||l_parameter||']');
  end if;

  pdebug(1, 'DEBUG ENABLED AT LEVEL '||:debug_level);
  if :debug_level >= 2 then
    pdebug(2, 'Variables after parameters check');
    print_variables;
  end if;  
  if :debug_level >= 3 then
    execute immediate 'select count(*) from user_tables where table_name = ''SPEXP_LOG''' into l_cnt;
    if l_cnt = 1 then
      execute immediate 'truncate table spexp_log';
      pdebug(3, 'Log table spexp_log is truncated');
    else
      execute immediate 'create table spexp_log (dt timestamp, msg varchar2(4000), info clob)';
      pdebug(3, 'Log table spexp_log is created');
    end if;
  end if;
  
  declare
    function modifier_to_time(i_modifier varchar2) return varchar2 is
    begin
      return case i_modifier when 'd' then 'day' when 'h' then 'hour' when 'm' then 'minute' when 's' then 'second' end;
    end;  
    procedure set_to_table_by_modifier (i_short varchar2, i_modifier varchar2) is
      l_dbname varchar2(11);
    begin
      case i_modifier 
        when 't' then 
          :to_table := i_short || to_char(sysdate, '_HH24MISS');
        when 'd' then 
          :to_table := i_short || to_char(sysdate, '_YYYYMMDD');
        when 'f' then
          :to_table := i_short || to_char(sysdate, '_YYYYMMDDHH24MISS');
        when 'x' then
          select '_' || substr(name, 1, 10) into l_dbname from v$database;
          :to_table := i_short || l_dbname || to_char(sysdate, '_YYMMDD_HH24MI');
        else
          err('unknown modifier ['||i_modifier||'], only t,d or f are supported');
      end case;
      pdebug(2, 'set to_table using modifier from short expr to ['||:to_table||']');
      :export_file_name := :to_table || '.sql';      
      pdebug(2, 'set export_file_name using modifier from short expr to ['||:export_file_name||']');
    end;
    procedure set_parameters_by_short (i_short varchar2) is
      l_numeric varchar2(10);
      l_modifier1 varchar2(1);
      l_modifier2 varchar2(2);
      l_start pls_integer;
      l_len pls_integer;
    begin
      l_start := length(i_short) + 1;
      l_len := length(:short);
      case 
        when l_start > l_len then
          :where_clause := '';
          pdebug(2, 'no where_clause by short expr, set to null');
        when l_start = l_len then
          :where_clause := '';
          pdebug(2, 'no where_clause by short expr, set to null');
          l_modifier1 := substr(:short, -1);
          set_to_table_by_modifier(i_short, l_modifier1);
        when i_short in ('ash', 'hash', 'sql') then
          l_modifier1 := substr(:short, -1);
          l_modifier2 := substr(:short, -2, 1);
          if instr('0123456789', l_modifier2) = 0 then
            set_to_table_by_modifier(i_short, l_modifier1);
            l_numeric := substr(:short, l_start, l_len - l_start - 1);
            l_modifier1 := l_modifier2;
          else 
            l_numeric := substr(:short, l_start, l_len - l_start);
          end if;
          if i_short = 'sql' then
            if l_modifier1 = 'x' then
              :max_clob_size := 1000*l_numeric;
              :sp_arraysize := 1;
              pdebug(2, 'set max_clob_size to ['||:max_clob_size||'] by short expr');
              pdebug(2, 'set sp_arraysize to ['||:sp_arraysize||'] by short expr');
            else  
              err('unknown modifier ['||l_modifier1||'], only x is supported');
            end if;
          else 
            :where_clause := 'sample_time>systimestamp-numtodsinterval('||l_numeric||','''||modifier_to_time(l_modifier1)||''')';
            pdebug(2, 'set where_clause to ['||:where_clause||'] by short expr');
          end if;
        else
          null;
      end case;
    end;
  begin
    if    :short is null then
      null;
    elsif :short = 'ses' then
      :from_table := 'gv$session';
      pdebug(2, 'set from_table to ['||:from_table||'] by short');
    elsif :short like 'ash%' then
      :from_table := 'gv$active_session_history';
      pdebug(2, 'set from_table to ['||:from_table||'] by short');
      set_parameters_by_short('ash');    
    elsif :short like 'hash%' then
      :from_table := 'dba_hist_active_sess_history';
      pdebug(2, 'set from_table to ['||:from_table||'] by short');
      set_parameters_by_short('hash');
    elsif :short like 'sql%' then
      :from_table := 'gv$sql';
      pdebug(2, 'set from_table to ['||:from_table||'] by short');
      set_parameters_by_short('sql');
    else
      err('Unknown short allias ['||:short||']');
    end if;
  end;
  
  if    :to_table is null and :from_table is not null then
    :to_table := substr('exp_'||replace(:from_table,'.','_'), 1, 30);
    pdebug(2, 'set to_table to ['||:to_table||'] by from_table by default');
  elsif :to_table is null and :from_query is not null then
    :to_table := 'query_'||to_char(sysdate, 'YYYYMMDDHH24MISS');
    pdebug(2, 'set to_table to ['||:to_table||'] by from_query by default');
  end if;

  if :export_file_name is null and :from_table is not null then
    :export_file_name := 'spexp_'||replace(:from_table,'$','_')||'.sql';
    pdebug(2, 'set export_file_name to ['||:export_file_name||'] by from_table');
  elsif :export_file_name is null and :from_query is not null then
    :export_file_name := 'spexp_query_'||to_char(sysdate, 'YYYYMMDDHH24MISS')||'.sql';
    pdebug(2, 'set export_file_name to ['||:export_file_name||'] by from_query');
  elsif :export_file_name not like '%.%' then
    :export_file_name := :export_file_name || '.sql';
    pdebug(2, 'set export_file_name to ['||:export_file_name||'] (add extention)');
  end if;
  
  if :os not in ('UNIX', 'WIN') then 
    err('unknown os type ['||:os||'], only UNIX and WIN are supported');
  elsif :os = 'WIN' and :use_gzip = 1 then
    err('You can not use gzip with os = WIN');
  end if;

  if :estimate = 1 then
    :no_create := 1;
    pdebug(2, 'set no_create to ['||:no_create||'] as estimate using');
    if :use_gzip = 1 then
      err('You can not use estimate and gzip in the same time');
    end if;
  end if;

  if :convert_characterset = 1 then
    declare
      function get_nls_param(i_param varchar2) return varchar2 is
        l_ret varchar2(30);
      begin
        execute immediate 'select value from v$nls_parameters where parameter = :param' into l_ret using i_param;
        return l_ret;
      end;
    begin
      :from_nls_characterset := get_nls_param('NLS_CHARACTERSET');
      :from_nls_nchar_characterset := get_nls_param('NLS_NCHAR_CHARACTERSET');
      :method_char := 'R';
      :method_clob := 'R';
      pdebug(2, 'set from_nls_characterset to ['||:from_nls_characterset||']');
      pdebug(2, 'set from_nls_nchar_characterset to ['||:from_nls_nchar_characterset||']');
      pdebug(2, 'set method_char to [R] as convert_characterset = 1');
      pdebug(2, 'set method_clob to [R] as convert_characterset = 1');
    end;
  end if;

  if :clob_as_char = 1 then
    :max_clob_size := least(:max_clob_size, 4000);
    pdebug(2, 'set max_clob_size to ['||:max_clob_size||'] as clob_as_char defined');
  end if;

  if :method_char not in ('N', 'R', 'S', 'A') then
    err('unknown char packing method ['||:method_char||'], only N, R, S or A are supported');
  end if;
  if :method_clob not in ('N', 'R', 'S', 'A') then
    err('unknown clob packing method ['||:method_clob||'], only N, R, S or A are supported');
  end if;  
  if (:method_char = 'S' or :method_clob = 'S') and :sub is null then
    err('empty sub with S method');
  end if;
  if :method_nchar not in ('R') then
    err('unknown nchar packing method ['||:method_nchar||'], only R is supported');
  end if;
  if :method_nclob not in ('R') then
    err('unknown nchar packing method ['||:method_nclob||'], only R is supported');
  end if;

  if :no_auto_temp = 0 then
    declare
      l_clob clob;
    begin
      l_clob := 'A';
    exception
      when others then
        :no_temp := 1;
        pdebug(2, 'set no_temp to 1 by detection');
    end;
  end if;
  
  if :no_temp = 1 then
    :clob_as_char := 1;
    :max_clob_size := :piece_size;
    :max_blob_size := :raw_piece_size;
    :method_char := case :method_char when 'S' then 'S' else 'N' end;
    pdebug(2, 'set clob_as_char to 1 as no_temp=1');
    pdebug(2, 'set max_clob_size to '||:max_clob_size||' as no_temp=1');
    pdebug(2, 'set max_blob_size to '||:max_blob_size||' as no_temp=1');
    pdebug(2, 'set method_char to '||:method_char||' as no_temp=1');
  end if;
  
  if &_ver < 11 then
    :no_clob_stmt := 1;
    pdebug(2, 'set no_clob_stmt to 1 by version '||&_ver);
  elsif :no_auto_clob_stmt = 0 and :no_clob_stmt = 0 and :no_temp = 0 then
    declare
      l_clob clob;
      l_cur sys_refcursor;
    begin
      l_clob := 'select 1 from dual';
      execute immediate 'begin open :l_cur for :l_clob; end;' using in out l_cur, in l_clob;
      if l_cur%isopen then
        close l_cur;
      end if;
    exception
      when others then
        :no_clob_stmt := 1;
        pdebug(2, 'set no_clob_stmt to 1 by detection');
    end;
  end if;
  
  if :no_auto_compress = 0 and :no_compress = 0 then
    declare
      l_blob blob;
    begin
      execute immediate 'select utl_compress.lz_compress(hextoraw(''FF'')) from dual' into l_blob;
    exception
      when others then
        :no_compress := 1;
        pdebug(2, 'set no_compress to 1 by detection');
    end;
  end if;
  
  if :no_auto_dbms_sql = 0 and :no_dbms_sql = 0 then
    begin
      execute immediate 'declare t dbms_sql.number_table; begin null; end;';
    exception
      when others then
        :no_dbms_sql := 1;
        pdebug(2, 'set no_dbms_sql to 1 by detection');
    end;
  end if;
  
  if :no_auto_dbms_lob = 0 and :no_dbms_lob = 0 then
    begin
      execute immediate 'declare t dbms_lob.blob_deduplicate_region; begin null; end;';
    exception
      when others then
        :no_dbms_lob := 1;
        pdebug(2, 'set no_dbms_lob to 1 by detection');
    end;
  end if;
  
  if :from_query is not null and :no_dbms_sql = 1 then
    err('export from query is unavailable as not dbms_sql access');
  end if;
  
  if :from_query is not null and :from_table is not null then
    err('both from_query and from_table/short can not be defined in the same time');
  end if;
  
  if :from_query is null and :from_table is null then
    :from_table := 'dual';
    :export_file_name := 'test_spexp_dual.sql';
    pdebug(2, 'set from_table to [dual] as default');
    pdebug(2, 'set from_table to [test_spexp_dual.sql] as default');
  end if;  
  
  if :from_table is not null then
    begin
      execute immediate 'select '||:hint_expr||' * from '||:from_table;
    exception
      when others then
        err('have the exception when attempt to check from_table : '||sqlerrm);
    end;
  end if;
  
  if :from_query is not null then
    begin
      execute immediate :from_query;
    exception
      when others then
        err('have the exception when attempt to check from_query : '||sqlerrm);
    end;
  end if;
  
  if :export_plsql_block_size > 0 and :export_prompt_size > 0 and :export_plsql_block_size <> :export_prompt_size then
    :export_prompt_size := :export_plsql_block_size;
    pdebug(2, 'set export_prompt_size to '||:export_plsql_block_size||' as export_plsql_block_size defined');
  end if;
  if :export_plsql_block_size > 0 and :commit_block_size > 0 and :export_plsql_block_size <> :commit_block_size then
    :commit_block_size := :export_plsql_block_size;
    pdebug(2, 'set commit_block_size to '||:export_plsql_block_size||' as export_plsql_block_size defined');
  end if; 

  if :debug_level >= 1 then
    pdebug(1, 'Variables after parsing');
    print_variables;
  end if;
  
end;
/

set termout off lin 100

select 
  case when :no_temp = 0 and :no_clob_stmt = 0 then 'clob' else 'varchar2(32767)' end tstr,
  case when :debug_level >= 1 then '  ' else '--' end debug1,
  case when :debug_level >= 2 then '  ' else '--' end debug2,
  case when :debug_level >= 3 then '  ' else '--' end debug3,
  case when :debug_level >= 3 then 'on' else 'off' end echo,
  :from_table tblname,
  :export_file_name || decode(:use_gzip, 1, '.gz') expfile,
  decode(:no_dbms_sql, 1, '--', '  ') dbmssql,
  decode(:no_dbms_lob, 1, '--', '  ') dbmslob,
  case when :os in ('UNIX') then '#' end nounix,
  case when :os in ('WIN') then 'REM' end nowin,
  :sp_arraysize sp_arraysize,
  :sp_long sp_long,
  decode(:use_gzip, 1, ' ', '#') use_gzip,
  decode(:use_gzip, 1, '&_tmpfifo', decode(:estimate, 1, '&_tmpfile1', :export_file_name)) spoolto
from dual;

set arraysize &_sp_arraysize long &_sp_long

spool &_tmpfile1
prompt declare
prompt &_debug3 pragma autonomous_transaction;;
prompt begin
prompt   if :from_table is null then return; end if;;
prompt   dbms_output.put_line('Start desc table');;
prompt   :descraw := regexp_replace('
desc &_tblname
prompt ', '[ ]+', ' ');;
prompt &_debug1 dbms_output.put_line('descraw size is ['||length(:descraw)||']');;
prompt &_debug3 insert into spexp_log values (systimestamp, ':descraw', :descraw); commit;;
prompt &_debug3 dbms_output.put_line('descraw has been saved into log table');;
prompt end;;
prompt /
spool off

set termout on lin 2498 

@&_tmpfile1

set echo &_echo

declare
&_debug3 pragma autonomous_transaction;
  type strs is table of &_tstr index by binary_integer;
  type nums is table of binary_integer index by binary_integer;
  str &_tstr;
  l_from varchar2(32767);
  col_names strs;
  col_types strs;
  col_length nums;
  col_formatted strs;
  
  procedure err(i_message varchar2) is
  begin
    dbms_output.put_line('Error: '||i_message);
    raise program_error;
  end;
  procedure pdebug(i_level pls_integer, i_message varchar2) is
  begin
    if i_level <= :debug_level then
      dbms_output.put_line(i_message);
    end if;
  end;
  
  procedure supported_types (i_col varchar2, io_type in out varchar2) is
  begin  
    if     io_type not like 'VARCHAR2%'
       and io_type not like 'NUMBER%'
       and io_type not like 'FLOAT%'
       and io_type not like 'DATE'
       and io_type not like 'RAW%'
       and io_type not like 'CHAR%'
       and io_type not like 'CLOB'
       and io_type not like 'BLOB'
       and io_type not like 'TIMESTAMP%'
       and io_type not like 'TIMESTAMP%WITH%TIME%ZONE%'
       and io_type not like 'TIMESTAMP%WITH%LOCAL%TIME%ZONE%'
       and io_type not like 'BINARY_FLOAT'
       and io_type not like 'BINARY_DOUBLE'
       and io_type not like 'ROWID'
       and io_type not like 'UROWID'
       and io_type not like 'INTERVAL%YEAR%TO%MONTH%'
       and io_type not like 'INTERVAL%DAY%TO%SECOND%'
       and io_type not like 'NVARCHAR2%'
       and io_type not like 'NCHAR%'
       and io_type not like 'NCLOB'
    then
      pdebug(0, 'column ['||i_col||'] can''t be exported as column type is not supported');   
      io_type := null;
    end if;
  end;

  procedure parse_descraw is
    l_row varchar2(300);
    l_fnd boolean := false;
    l_cnt  pls_integer := 0;
    l_pos1 pls_integer := 0;
    l_pos2 pls_integer;
    l_middle pls_integer;
    l_name varchar2(30);
    l_type varchar2(40);
    l_length_t varchar2(100);
    l_length pls_integer;
    l_lbr pls_integer;
    l_rbr pls_integer;
  begin
    pdebug(0, 'Start parsing desc table');
    loop
      l_pos2 := instr(:descraw, chr(10), l_pos1 + 1);
      exit when nvl(l_pos2, 0) = 0;
      l_row := trim(substr(:descraw, l_pos1 + 1, l_pos2 - l_pos1 - 1));
      if l_fnd then
        l_middle := instr(l_row, ' ');
        if l_middle > 1 then
          l_name := substr(l_row, 1, l_middle - 1);
          pdebug(2, 'get column name ['||l_name||']');
          if :pmcols = 0 or (:pmcols = 1 and :cols like '%,'||l_name||'%') or (:pmcols = -1 and :cols not like '%,'||l_name||'%') then
            l_cnt := l_cnt + 1;
            pdebug(2, 'column number is #'||l_cnt);
            l_type := ltrim(replace(substr(l_row, l_middle), 'NOT NULL', ''));
            pdebug(2, 'get column type ['||l_type||']');
            supported_types(l_name, l_type);
            col_names(l_cnt) := l_name;
            if l_type = 'CLOB' then
              pdebug(2, 'use max_clob_size as length for CLOBs ['||:max_clob_size||']');
              col_length(l_cnt) := :max_clob_size;
            elsif l_type = 'BLOB' then
              pdebug(2, 'use max_blob_size as length for BLOBs ['||:max_blob_size||']');
              col_length(l_cnt) := :max_blob_size;
              if :no_dbms_lob = 1 then 
                pdebug(0, 'column ['||l_name||'] can''t be exported as no access to dbms_lob');
              end if;
            elsif l_type like 'NUMBER%' then
              l_type := 'NUMBER';
              pdebug(2, 'set no length for number subtypes');
              col_length(l_cnt) := null;
            elsif l_type like 'ROWID' then
              l_type := 'UROWID(4000)';
              pdebug(2, 'rowid is exported as urowid');
              col_length(l_cnt) := 4000;
            elsif l_type like '%(%)%' then
              l_lbr := instr(l_type, '(');
              l_rbr := instr(l_type, ')');
              l_length_t := substr(l_type, l_lbr + 1, l_rbr - l_lbr - 1);
              if l_length_t like '%CHAR' then
                l_length := least(4 * substr(l_length_t, 1, instr(l_length_t, ' ') - 1), 4000);
              else
                l_length := l_length_t;
              end if;
              pdebug(2, 'get column length ['||l_length||']');
              col_length(l_cnt) := l_length;
            else
              pdebug(2, 'no length');
              col_length(l_cnt) := null;
            end if;
            col_types(l_cnt) := l_type;
          else
            pdebug(2, 'column is not exported due to plus_cols/minus_cols parameter value');
          end if;
        end if;
      else
        l_fnd := l_row like '%---------%';
      end if;  
      l_pos1 := l_pos2;
    end loop;
    if col_types.count = 0 then
      err('No columns for export, check plus_cols/minus_cols parameter');
    end if;
  end;
  
&_dbmssql  procedure describe_query is
&_dbmssql    l_cur pls_integer;
&_dbmssql    l_cnt pls_integer := 0;
&_dbmssql    l_colcnt pls_integer;
&_dbmssql    l_desc dbms_sql.desc_tab;
&_dbmssql    l_type varchar2(30);
&_dbmssql    l_cfrm pls_integer;
&_dbmssql    function check_characterset(i_type varchar2, i_cfrm pls_integer) return varchar2 is
&_dbmssql    begin
&_dbmssql      case i_cfrm
&_dbmssql        when 1 then return i_type;
&_dbmssql        when 2 then return 'N'||i_type;
&_dbmssql        else
&_dbmssql          pdebug(0, 'Found unknown col_charsetform ['||i_cfrm||']');
&_dbmssql          return null;
&_dbmssql      end case;
&_dbmssql    end;
&_dbmssql  begin
&_dbmssql    pdebug(0, 'Start describing columns');
&_dbmssql    l_cur := dbms_sql.open_cursor;
&_dbmssql    dbms_sql.parse(l_cur, :from_query, dbms_sql.native);
&_dbmssql    dbms_sql.describe_columns(l_cur, l_colcnt, l_desc);
&_dbmssql    for i in 1 .. l_colcnt loop
&_dbmssql      pdebug(2, 'get column name ['||l_desc(i).col_name||']');
&_dbmssql      if :pmcols = 0 or (:pmcols = 1 and :cols like '%,'||l_desc(i).col_name||'%') or (:pmcols = -1 and :cols not like '%,'||l_desc(i).col_name||'%') then
&_dbmssql        l_cnt := l_cnt + 1;
&_dbmssql        pdebug(2, 'column number is #'||l_cnt);
&_dbmssql        col_names(l_cnt) := l_desc(i).col_name;
&_dbmssql        l_cfrm := l_desc(i).col_charsetform;
&_dbmssql        l_type:= case l_desc(i).col_type
&_dbmssql          when 1   then check_characterset('VARCHAR2'||'('||l_desc(i).col_max_len||')', l_cfrm)
&_dbmssql          when 2   then 'NUMBER'
&_dbmssql          when 12  then 'DATE'
&_dbmssql          when 23  then 'RAW'||'('||l_desc(i).col_max_len||')'
&_dbmssql          when 96  then check_characterset('CHAR'||'('||l_desc(i).col_max_len||')', l_cfrm)
&_dbmssql          when 112 then check_characterset('CLOB', l_cfrm)
&_dbmssql          when 113 then 'BLOB'
&_dbmssql          when 180 then 'TIMESTAMP(9)'
&_dbmssql          when 181 then 'TIMESTAMP(9) WITH TIME ZONE'
&_dbmssql          when 231 then 'TIMESTAMP(9) WITH LOCAL TIME ZONE'
&_dbmssql          when 100 then 'BINARY_FLOAT'
&_dbmssql          when 101 then 'BINARY_DOUBLE'
&_dbmssql          when 11  then 'ROWID'
&_dbmssql          when 208 then 'UROWID'
&_dbmssql          when 182 then 'INTERVAL YEAR(9) TO MONTH'
&_dbmssql          when 183 then 'INTERVAL DAY(9) TO SECOND(9)'
&_dbmssql        end;
&_dbmssql        pdebug(2, 'get column type ['||l_type||']');
&_dbmssql        supported_types(l_desc(i).col_name, l_type);
&_dbmssql        col_types(l_cnt) := l_type;
&_dbmssql        if l_type = 'CLOB' then
&_dbmssql          pdebug(2, 'use max_clob_size as length for CLOBs ['||:max_clob_size||']');
&_dbmssql          col_length(i) := :max_clob_size;
&_dbmssql        elsif l_type = 'BLOB' then
&_dbmssql          pdebug(2, 'use max_blob_size as length for BLOBs ['||:max_blob_size||']');
&_dbmssql          col_length(i) := :max_blob_size;
&_dbmssql          if :no_dbms_lob = 1 then 
&_dbmssql            pdebug(0, 'column ['||l_desc(i).col_name||'] can''t be exported as no access to dbms_lob');
&_dbmssql          end if;
&_dbmssql        else
&_dbmssql          pdebug(2, 'get column length ['||l_desc(i).col_max_len||']');
&_dbmssql          col_length(i) := l_desc(i).col_max_len;
&_dbmssql        end if;
&_dbmssql      else
&_dbmssql        pdebug(2, 'column is not exported due to plus_cols/minus_cols parameter value');
&_dbmssql      end if;
&_dbmssql    end loop;
&_dbmssql    if col_types.count = 0 then
&_dbmssql      err('No columns for export, check plus_cols/minus_cols parameter');
&_dbmssql    end if;
&_dbmssql  end;

  function pack_piece_method_n (i_col varchar2, i_piece_size pls_integer, i_pos pls_integer) return varchar2 is
  begin
    return '''''''''||replace(replace(to_char(substr('||i_col||','||i_pos||','||i_piece_size||')), '''''''', ''''''''''''),chr(10),''''''||:n||'''''')||''''''''';
  end;

  function pack_piece_method_s (i_col varchar2, i_piece_size pls_integer, i_pos pls_integer) return varchar2 is
  begin
    return '''replace(''''''||replace(replace(to_char(substr('||i_col||','||i_pos||','||i_piece_size||')), '''''''', ''''''''''''),chr(10),'''||:sub||''')||'''''',:s,:n)''';
  end;

  function pack_piece_method_r (i_col varchar2, i_piece_size pls_integer, i_pos pls_integer, i_max_size pls_integer, i_is_nchar pls_integer, i_compress_min_size pls_integer default :compress_min_size) return varchar2 is
    l_to_char0 varchar2(30);
    l_to_char1 varchar2(100);
    l_to_char2 varchar2(100);
  begin
    l_to_char0 := case i_is_nchar when 1 then 'to_nchar' else 'to_char' end;
    l_to_char1 := case when :convert_characterset = 0 and i_is_nchar = 0 then 'utl_raw.cast_to_varchar2' when :convert_characterset = 0 and i_is_nchar = 1 then 'utl_raw.cast_to_nvarchar2'
                       when :convert_characterset = 1 and i_is_nchar = 0 then 'utl_i18n.raw_to_char'     when :convert_characterset = 1 and i_is_nchar = 1 then 'utl_i18n.raw_to_nchar' end;
    l_to_char2 := case when :convert_characterset = 1 and i_is_nchar = 0 then ',:c'                      when :convert_characterset = 1 and i_is_nchar = 1 then ',:nc' end;
    if :no_compress = 0 and i_compress_min_size > 0 and i_max_size > i_compress_min_size then
      return ''''||l_to_char1||'(''||case when length('||i_col||')>'||(i_pos + i_compress_min_size)||' then ''utl_compress.lz_uncompress'' end||''(hextoraw(''''''||case when length('
      ||i_col||')>'||(i_pos + i_compress_min_size)||' then rawtohex(utl_compress.lz_compress(hextoraw(rawtohex('||l_to_char0||'(substr('
      ||i_col||','||i_pos||','||i_piece_size||'))))))else rawtohex('||l_to_char0||'(substr('||i_col||','||i_pos||','||i_piece_size||')))end||''''''))'||l_to_char2||')''';
    elsif :no_compress = 0 and i_compress_min_size = 0 then 
      return ''''||l_to_char1||'(utl_compress.lz_uncompress(hextoraw(''''''||rawtohex(utl_compress.lz_compress(hextoraw(rawtohex('||l_to_char0||'(substr('
      ||i_col||','||i_pos||','||i_piece_size||'))))))||''''''))'||l_to_char2||')''';
    else
      return ''''||l_to_char1||'(hextoraw(''''''||rawtohex('||l_to_char0||'(substr('||i_col||','||i_pos||','||i_piece_size||')))||'''''')'||l_to_char2||')''';
    end if;
  end;

  function pack_piece_method_a (i_col varchar2, i_piece_size pls_integer, i_pos pls_integer, i_max_size pls_integer) return varchar2 is
  begin
    if i_max_size > :raw_method_min_size then
      return 'case when length('||i_col||')>'||(i_pos + :compress_min_size)||' then'||pack_piece_method_r(i_col, i_piece_size, i_pos, i_max_size, 0)
      ||' else '||pack_piece_method_n(i_col, i_piece_size, i_pos)||' end ';
    else
      return pack_piece_method_n(i_col, i_piece_size, i_pos);
    end if;
  end;

  function pack_piece (i_col varchar2, i_piece_size pls_integer, i_pos pls_integer, i_method varchar2, i_max_size pls_integer, i_is_nchar pls_integer) return varchar2 is
  begin
    return
      case i_method
        when 'N' then pack_piece_method_n(i_col, i_piece_size, i_pos)
        when 'S' then pack_piece_method_s(i_col, i_piece_size, i_pos)
        when 'R' then pack_piece_method_r(i_col, i_piece_size, i_pos, i_max_size, i_is_nchar)
        when 'A' then pack_piece_method_a(i_col, i_piece_size, i_pos, i_max_size)
      end;
  end;

  function pack_char(i_col varchar2, i_max_size pls_integer, i_method varchar2, i_is_nchar pls_integer) return str%type is
    l_res &_tstr;
    l_piece_size pls_integer;
    l_res_buff varchar2(32767);
    l_num_pieces pls_integer;
    l_pos pls_integer;
  begin
    l_piece_size := case when i_method in ('R', 'A') then :raw_method_piece_size else :piece_size end;
    l_num_pieces := ceil(i_max_size / l_piece_size);
    l_res := pack_piece(i_col, l_piece_size, 1, i_method, i_max_size, i_is_nchar);
    for i in 2 .. l_num_pieces loop
      l_pos := l_piece_size * (i - 1) + 1;
      l_res_buff := l_res_buff || 'a,''||''||' || pack_piece(i_col, l_piece_size, l_pos, i_method, i_max_size, i_is_nchar);
      if length(l_res_buff) > 32000 then
        l_res := l_res || l_res_buff;
        l_res_buff := '';
      end if;
    end loop;
    l_res := l_res || l_res_buff;
    return l_res;
  end;

  function pack_clob(i_col varchar2, i_max_size pls_integer, i_method varchar2, i_is_nchar pls_integer) return str%type is
    l_res &_tstr;
    l_piece_size pls_integer;
    l_res_buff varchar2(32767);
    l_num_pieces pls_integer;
    l_pos pls_integer;
  begin
    l_piece_size := case when i_method in ('R', 'A') then :raw_method_piece_size else :piece_size end;
    l_num_pieces := ceil(i_max_size / l_piece_size);
    l_res := case i_is_nchar when 0 then '''(to_clob(null)||''||' else '''(to_nclob(null)||''||' end;
    l_res := l_res || '''(''||' || pack_piece(i_col, l_piece_size, 1, i_method, i_max_size, i_is_nchar) || '||'')||''a';
    for i in 2 .. l_num_pieces - 1 loop
      l_pos := l_piece_size * (i - 1) + 1;
      if :no_optimize_clob_tail = 0 then
        l_res_buff := l_res_buff || ',case when length('||i_col||')>='||l_pos||' then ''(''||' || pack_piece(i_col, l_piece_size, l_pos, i_method, i_max_size, i_is_nchar)||'||'')||'' else ''--'' end a';
      else
        l_res_buff := l_res_buff || ',''(''||' || pack_piece(i_col, l_piece_size, l_pos, i_method, i_max_size, i_is_nchar)||'||'')||''a';
      end if;
      if i mod 256 = 0 then
        l_res_buff := l_res_buff || case i_is_nchar when 0 then ',''null)||(to_clob(null)||''a' else ',''null)||(to_nclob(null)||''a' end;
      end if;
      if length(l_res_buff) > 32000 then
        l_res := l_res || l_res_buff;
        l_res_buff := '';
      end if;
    end loop;
    l_res := l_res || l_res_buff;
    if l_num_pieces > 1 then
      l_res := l_res || ',' || pack_piece(i_col, l_piece_size, l_pos + l_piece_size, i_method, i_max_size, i_is_nchar) || '||'')''';
    else
      l_res := l_res || ','''''''''')''';
    end if;
    return l_res;
  end;

  function pack_raw_piece(i_col varchar2, i_piece_size pls_integer, i_pos pls_integer, i_max_size pls_integer) return varchar2 is
    l_pos pls_integer;
    l_piece_size pls_integer;
  begin
    l_pos := 2 * (i_pos - 1) + 1;
    l_piece_size := 2 * i_piece_size;
    if :no_compress = 0 and :no_raw_compress = 0 and i_max_size > :raw_compress_min_size then
      return 'case when lengthb('||i_col||')>'||(l_pos + 2 * :raw_compress_min_size)||' then ''utl_compress.lz_uncompress'' end||''(hextoraw(''''''||case when lengthb('
      ||i_col||')>'||(l_pos + 2 * :raw_compress_min_size)||' then utl_compress.lz_compress(hextoraw(substr(rawtohex('||i_col||'),'||l_pos||','||l_piece_size||
      ')))else hextoraw(substr(rawtohex('||i_col||'),'||l_pos||','||l_piece_size||'))end||''''''))''';
    else
      return '''''''''||substr(rawtohex('||i_col||'),'||l_pos||','||l_piece_size||')||''''''''';
    end if;
  end;

  function pack_raw(i_col varchar2, i_max_size pls_integer) return str%type is
    l_res &_tstr;
    l_res_buff varchar2(32767);
    l_num_pieces pls_integer;
    l_pos pls_integer;
  begin
    l_num_pieces := ceil(i_max_size / :raw_piece_size);
    l_res := pack_raw_piece(i_col, :raw_piece_size, 1, i_max_size);
    for i in 2 .. l_num_pieces loop
      l_pos := :raw_piece_size * (i - 1) + 1;
      l_res_buff := l_res_buff || 'a,''||''||' || pack_raw_piece(i_col, :raw_piece_size, l_pos, i_max_size);
      if length(l_res_buff) > 32500 then
        l_res := l_res || l_res_buff;
        l_res_buff := '';
      end if;
    end loop;
    l_res := l_res || l_res_buff;
    return l_res;    
  end;

&_dbmslob  function pack_blob_piece(i_col varchar2, i_piece_size pls_integer, i_pos pls_integer, i_max_size pls_integer) return varchar2 is
&_dbmslob  begin
&_dbmslob    if :no_compress = 0 and :no_raw_compress = 0 and i_max_size > :raw_compress_min_size then
&_dbmslob      return 'case when lengthb('||i_col||')>'||(2 * (i_pos + :raw_compress_min_size))||' then ''utl_compress.lz_uncompress'' end||''(hextoraw(''''''||case when lengthb('
&_dbmslob      ||i_col||')>'||(2 * (i_pos + :raw_compress_min_size))||' then utl_compress.lz_compress(dbms_lob.substr('||i_col||','||i_piece_size||','||i_pos||
&_dbmslob      '))else dbms_lob.substr('||i_col||','||i_piece_size||','||i_pos||')end||''''''))''';
&_dbmslob    else
&_dbmslob      return '''''''''||dbms_lob.substr('||i_col||','||i_piece_size||','||i_pos||')||''''''''';
&_dbmslob    end if;
&_dbmslob  end;
  
&_dbmslob  function pack_blob(i_col varchar2, i_max_size pls_integer) return str%type is
&_dbmslob    l_res &_tstr;
&_dbmslob    l_raw_piece_size pls_integer;
&_dbmslob    l_res_buff varchar2(32767);
&_dbmslob    l_num_pieces pls_integer;
&_dbmslob    l_pos pls_integer;
&_dbmslob  begin
&_dbmslob    l_num_pieces := ceil(i_max_size / :raw_piece_size);
&_dbmslob    l_raw_piece_size := floor(i_max_size / l_num_pieces);
&_dbmslob    l_res := pack_blob_piece(i_col, l_raw_piece_size, 1, i_max_size);
&_dbmslob    for i in 2 .. l_num_pieces loop
&_dbmslob      l_pos := l_raw_piece_size * (i - 1) + 1;
&_dbmslob      l_res_buff := l_res_buff || 'a,''||''||' || pack_blob_piece(i_col, l_raw_piece_size, l_pos, i_max_size);
&_dbmslob      if length(l_res_buff) > 32500 then
&_dbmslob        l_res := l_res || l_res_buff;
&_dbmslob        l_res_buff := '';
&_dbmslob      end if;
&_dbmslob    end loop;
&_dbmslob    l_res := l_res || l_res_buff;
&_dbmslob    return l_res;
&_dbmslob  end;

  procedure apply_formats is
    l_col_formatted &_tstr;
  begin
    pdebug(0, 'Start applying format for columns');
    for i in 1 .. col_names.count loop
      l_col_formatted := case
        when col_types(i) like 'VARCHAR2%' or col_types(i) like 'CHAR%' or (:clob_as_char = 1 and col_types(i) = 'CLOB')
          then pack_char(col_names(i), col_length(i), :method_char, 0)
        when col_types(i) = 'CLOB'
          then pack_clob(col_names(i), col_length(i), :method_clob, 0)
        when col_types(i) like 'NVARCHAR2%' or col_types(i) like 'NCHAR%' or (:clob_as_char = 1 and col_types(i) = 'NCLOB')
          then pack_char(col_names(i), col_length(i), :method_nchar, 1)
        when col_types(i) = 'NCLOB'
          then pack_clob(col_names(i), col_length(i), :method_nclob, 1)
        when col_types(i) like 'RAW%'
          then pack_raw(col_names(i), col_length(i))
&_dbmslob when col_types(i) = 'BLOB'
&_dbmslob   then pack_blob(col_names(i), col_length(i))
        when col_types(i) like 'NUMBER%' or col_types(i) like 'FLOAT%'
          then 'nvl(to_char('||col_names(i)||', ''tm9''), '''''''''''')'
        when col_types(i) like 'DATE%'
          then '''to_date(''''''||to_char('||col_names(i)||', ''YYYYMMDDHH24MISS'')||'''''',''''YYYYMMDDHH24MISS'''')'''
        when col_types(i) like 'TIMESTAMP%WITH%TIME%ZONE'
          then '''to_timestamp_tz(''''''||to_char('||col_names(i)||', ''YYYYMMDDHH24MISSFF TZR'')||'''''',''''YYYYMMDDHH24MISSFF TZR'''')'''
        when col_types(i) like 'TIMESTAMP%'
          then '''to_timestamp(''''''||to_char('||col_names(i)||', ''YYYYMMDDHH24MISSFF'')||'''''',''''YYYYMMDDHH24MISSFF'''')'''
        when col_types(i) = 'BINARY_FLOAT'
          then '''-utl_raw.cast_to_binary_float(hextoraw(''''''||rawtohex('||col_names(i)||')||''''''))'''
        when col_types(i) = 'BINARY_DOUBLE'
          then '''-utl_raw.cast_to_binary_double(hextoraw(''''''||rawtohex('||col_names(i)||')||''''''))'''
        when col_types(i) = 'ROWID'
          then pack_char(col_names(i), 13, :method_char, 0)
        when col_types(i) like 'UROWID%'
          then pack_char(col_names(i), col_length(i), :method_char ,0)
        when col_types(i) like 'INTERVAL%YEAR%TO%MONTH%'
          then '''to_yminterval(''''''||'||col_names(i)||'||'''''')'''
        when col_types(i) like 'INTERVAL%DAY%TO%SECOND%'
          then '''to_dsinterval(''''''||'||col_names(i)||'||'''''')'''
        else ''''''''''''''
      end;
      pdebug(2, 'formatted column #'||i||' is ['||substr(l_col_formatted, 1, 100)||case when length(l_col_formatted)>100 then ' ...]' else ']' end);
&_debug3 insert into spexp_log values (systimestamp, 'formatted column #'||i, l_col_formatted); commit;
      pdebug(3, 'full formatted column #'||i||' has been saved into log table');
      col_formatted(i) := l_col_formatted;
    end loop;
  end;
  
  procedure create_exp_cursor (i_from varchar2, o_cur out sys_refcursor) is
    l_stmt &_tstr;
    l_open_block varchar2(1000);
    l_close_block varchar2(1000);
  begin
    pdebug(0, 'Start creating exporting cursor');
    l_open_block := case :export_plsql_block_size  when 0 then '' else 'decode(mod(rownum-1,'||:export_plsql_block_size||'),0,''begin''||chr(10))||' end;
    l_close_block := case :export_plsql_block_size when 0 then '' else '||decode(mod(rownum,'||:export_plsql_block_size||'),0,chr(10)||''end;''||chr(10)||''/'')' end||
                     case :commit_block_size       when 0 then '' else '||decode(mod(rownum,'||:commit_block_size||'),0,chr(10)||''commit;'')' end ||
                     case :export_prompt_size      when 0 then '' else '||decode(mod(rownum,'||:export_prompt_size||'),0,chr(10)||''set termout on''||chr(10)||''prompt ''||rownum||'' rows imported''||chr(10)||''set termout off'')' end;
    l_stmt := 'select '||:hint_expr||' '||l_open_block||'''insert into ' || :to_table || ' values ('' a,';
    l_stmt := l_stmt || col_formatted(1) || ' a';
    for i in 2 .. col_formatted.count loop
      l_stmt := l_stmt || ', '',''||' || col_formatted(i) || ' a';
    end loop;
    l_stmt := l_stmt || ', '');'''||l_close_block||' a from '||i_from||' a';
    if :where_clause is not null then
      l_stmt := l_stmt || ' where ' || :where_clause;
    end if;
    pdebug(2, 'exporting cursor query length is ['||length(l_stmt)||']');
&_debug3 insert into spexp_log values (systimestamp, 'exporting cursor query', l_stmt); commit;
    pdebug(3, 'exporting cursor query has been saved into log table');
    open o_cur for l_stmt;
  end;
  
  procedure create_tbl_cursor(o_cur out sys_refcursor) is
    l_stmt &_tstr;
    function extend_types(i_type varchar2) return varchar2 is
    begin
      if :convert_characterset = 1 then
        if i_type like 'CHAR%' then return 'CHAR'||regexp_substr(i_type, '\([0-9]+')||' CHAR)';
        elsif i_type like 'VARCHAR2%' then return 'VARCHAR2'||regexp_substr(i_type, '\([0-9]+')||' CHAR)';
        else return i_type;
        end if;
      else 
        return i_type;
      end if;
    end;
  begin
    pdebug(0, 'Start creating table creating cursor');
    if :drop_before_create = '1' then
      l_stmt := 'drop table '||:to_table||' purge;'||chr(10);
    end if;
    if :no_create = 0 then
      l_stmt := l_stmt || 'create table '||:to_table||' ('||chr(10);
      l_stmt := l_stmt || '  "' || col_names(1) || '" ' || nvl(extend_types(col_types(1)), 'VARCHAR2(1)');
      for i in 2 .. col_names.count loop
        l_stmt := l_stmt || ',' || chr(10) || '  "' || col_names(i) || '" ' || nvl(extend_types(col_types(i)), 'VARCHAR2(1)');
      end loop;
      l_stmt := l_stmt || ');';
    end if;
    pdebug(2, 'table creating cursor query length is ['||length(l_stmt)||']');
&_debug3 insert into spexp_log values (systimestamp, 'table creating cursor query', l_stmt); commit;
    pdebug(3, 'table creating cursor query has been saved into log table');
    open o_cur for select l_stmt a from dual;
  end;
  
  procedure print_row_count(i_from varchar2) is
  begin
    pdebug(0, 'Start counting exported rows');
    execute immediate 'select '||:hint_expr||' count(*) from '||i_from||' a'||case when :where_clause is not null then ' where ' || :where_clause end into :row_count;
    pdebug(0, 'Rows to export : '||:row_count);
  end;  

begin
  if :descraw is not null then
    parse_descraw;
    l_from := :from_table;
&_dbmssql  else 
&_dbmssql    describe_query;
&_dbmssql    l_from := '('||:from_query||')';
  end if;
  if :estimate = 1 then
    print_row_count(l_from);
    :where_clause := :where_clause || case when :where_clause is not null then ' and ' end || 'rownum <= '||:estimate_rows_to_check;
  end if;
  apply_formats;
  create_tbl_cursor(:ddl_cur);
  create_exp_cursor(l_from, :data_cur);
end;
/

set echo off

select 'Start spooling file &_spoolto' from dual where :estimate = 0;

set termout off
set define %
host %_nowin %_use_gzip mkfifo %_tmpfifo ; gzip -9 -c < %_tmpfifo > %_expfile &
set define &
exec :t_start := to_char(systimestamp, 'YYYYMMDDHH24MISSFF')
spool &_spoolto
prompt set termout off
prompt store set plusenv.tmp.sql replace
prompt set termout on
prompt prompt
prompt prompt *** spexp.sql v1.0 beta - (c) Valentin Nikotin (valentinnikotin.com) ***
prompt prompt
prompt prompt Start DDLs
prompt set termout off define off feedback off
prompt whenever sqlerror exit rollback
print ddl_cur
prompt set termout on
select 'prompt Start data import into table '||:to_table from dual;
prompt set termout off
prompt alter session set cursor_sharing = force;;
prompt var n varchar2(1)
prompt exec :n := chr(10)
select 'var s varchar2(13)'||chr(10)||'exec :s := '''||:sub||'''' from dual where :sub is not null;
select 'var c varchar2(30)'||chr(10)||'exec :c := '''||:from_nls_characterset||''''||chr(10)||'var nc varchar2(30)'||chr(10)||'exec :nc := '''||:from_nls_nchar_characterset||'''' from dual where :convert_characterset = 1;
print data_cur
select case :export_plsql_block_size when 0 then '' else 'end;'||chr(10)||'/' end from dual;
prompt commit;;
prompt alter session set cursor_sharing = exact;;
prompt set termout on
prompt prompt Import is done
prompt set termout off
prompt start plusenv.tmp.sql
prompt whenever sqlerror continue
prompt set termout on
spool off
exec :t_end := to_char(systimestamp, 'YYYYMMDDHH24MISSFF')
host &_nowin &_use_gzip rm &_tmpfifo

spool &_tmpfile2
prompt exec :f_size := null
spool off
select '&_expfile' spoolto from dual where :estimate = 0;
host &_nowin echo "exec :f_size := regexp_substr('"`du -b &_spoolto`"', '^[0-9]+')" > &_tmpfile2
@&_tmpfile2

set termout on
begin
  dbms_output.put_line('Elapsed time : '||(to_timestamp(:t_end, 'YYYYMMDDHH24MISSFF') - to_timestamp(:t_start, 'YYYYMMDDHH24MISSFF')));
  dbms_output.put_line('File size : '||:f_size);
  if :estimate = 1 then
    dbms_output.put_line('Estimated time to export : '||(to_timestamp(:t_end, 'YYYYMMDDHH24MISSFF') - to_timestamp(:t_start, 'YYYYMMDDHH24MISSFF'))*:row_count/:estimate_rows_to_check);
    dbms_output.put_line('Estimated size of export : '||round(:f_size*:row_count/:estimate_rows_to_check));
  end if;
end;
/

select 'Object/query has been exported into file &_expfile' from dual where :estimate = 0;

set termout off
spool &_tmpfile1
select sys_connect_by_path('col '||level||' clear','
') a from dual where level=&_max_parameters_count connect by level<=&_max_parameters_count;
spool off
@&_tmpfile1
col tmpfile1 new_value clear
col tmpfile2 new_value clear
col tblname new_value clear
col expfile new_value clear
col debug1 new_value clear
col debug2 new_value clear
col debug3 new_value clear
col tstr new_value clear
col dbmssql new_value clear
col dbmslob new_value clear
col nounix new_value clear
col nowin new_value clear
col sp_arraysize clear
col sp_long clear
col echo clear
col tmpfifo clear
col use_gzip clear
col spoolto clear
col a clear
col ver clear
start plusenv.tmp.sql
host &_nowin rm &_tmpfile1 &_tmpfile2 plusenv.tmp.sql
host &_nounix del &_tmpfile1 &_tmpfile2 plusenv.tmp.sql
undefine &_all_params _all_params _max_parameters_count _tmpfile1 _tmpfile2 _tblname _expfile _debug1 _debug2 _debug3 _tstr _dbmssql _dbmslob _nounix _nowin _sp_arraysize _sp_long _echo _tmpfifo _use_gzip _spoolto _ver
whenever sqlerror continue
set termout on
