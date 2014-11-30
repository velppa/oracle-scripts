CREATE OR REPLACE PACKAGE COMP_SYS.pkg_part_mgmt
AS
/*
   * v.0.5
   * Пакет для управления партиционированными объектами БД

   === История версий ===
   2012-07-22 | 0.1  | Попов П.В. | * создание пакета
   2012-07-23 | 0.11 | Попов П.В. | * в процедуре move_with_compression добавлено 
                                      создание партиций в нужном табличном пространстве
   2012-07-23 | 0.2  | Попов П.В. | * переписана процедура execute
                                    * добавлена процедура удаления табличного пространства
   2012-07-27 | 0.3  | Попов П.В. | * процедура добавления партиций в range partitioned таблицы
                                      теперь добавляет партиции для каждого номера в диапазоне
                                    * процедура execute при возникновении исключения OBJECT BUSY
                                      ждет 60 секунд и пытается повторить заново. Максимально - 25 попыток   
   2012-07-30 | 0.4  | Попов П.В. | * изменена процедура move_with_compression - часть  
                                      функционала перенесена из процедуры в поток wf_DDL
   2012-08-03 | 0.5  | Попов П.В. | * Перенос в схему COMP_SYS, подержка указания схемы 
                                      в имени таблицы
   2012-08-09 | 0.6  | Попов П.В. | * Процедура create_tablespace теперь, при длине входного параметра
                                      больше 30 символов обрезает его до 30 символов
                                    * Процедура move_with_compression теперь перестраивает индексы 
                                      у целевых таблиц
   2012-08-16 | 0.7  | Попов П.В. | * Из процедуры move_with_compression убрана перестройка индексов
*/

  TYPE columns_type IS RECORD(insert_cols VARCHAR2(2000)
                             ,select_cols VARCHAR2(2000));

  FUNCTION partition_exists(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_num INTEGER) RETURN BOOLEAN;

  FUNCTION partition_exists(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE) RETURN BOOLEAN;

  PROCEDURE truncate_partition(in_owner VARCHAR2 DEFAULT USER
                              ,in_tab_name VARCHAR2
                              ,in_num INTEGER);
  PROCEDURE truncate_partition(in_owner VARCHAR2 DEFAULT USER
                              ,in_tab_name VARCHAR2
                              ,in_date_to DATE);

  PROCEDURE add_partition(in_owner VARCHAR2 DEFAULT USER
                         ,in_tab_name VARCHAR2
                         ,in_date_to DATE
                         ,in_type VARCHAR2 DEFAULT 'range'
                         ,in_opts VARCHAR2 DEFAULT NULL);
  PROCEDURE add_partition(in_owner VARCHAR2 DEFAULT USER
                         ,in_tab_name VARCHAR2
                         ,in_num INTEGER
                         ,in_type VARCHAR2 DEFAULT 'range'
                         ,in_opts VARCHAR2 DEFAULT NULL);

  PROCEDURE drop_partition(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_num INTEGER);
  PROCEDURE drop_partition(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_to DATE);

  PROCEDURE create_tablespace(io_tablespace_name IN OUT VARCHAR2);

  PROCEDURE disable_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2 
                           ,in_num INTEGER);
  PROCEDURE disable_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE);

  PROCEDURE enable_indexes(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_num INTEGER);
  PROCEDURE enable_indexes(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_to DATE);

  PROCEDURE rebuild_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_num INTEGER
                           ,in_tablespace_name VARCHAR2 DEFAULT NULL);
  PROCEDURE rebuild_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE
                           ,in_tablespace_name VARCHAR2 DEFAULT NULL);

  FUNCTION common_columns(in_owner_src VARCHAR2 DEFAULT USER
                         ,in_tab_name_src VARCHAR2
                         ,in_owner_trg VARCHAR2 DEFAULT USER
                         ,in_tab_name_trg VARCHAR2) RETURN columns_type;

  PROCEDURE make_range(in_owner VARCHAR2 DEFAULT USER
                      ,in_tab_name VARCHAR2
                      ,in_date_from DATE
                      ,in_date_to DATE
                      ,in_create_tablespace INTEGER DEFAULT 0);

  PROCEDURE truncate_range(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_from DATE
                          ,in_date_to DATE);

  PROCEDURE move_with_compression(in_owner_src VARCHAR2 DEFAULT USER
                                 ,in_tab_name_src VARCHAR2
                                 ,in_owner_trg VARCHAR2 DEFAULT USER
                                 ,in_tab_name_trg VARCHAR2
                                 ,in_num INTEGER
                                 ,in_mode VARCHAR2 DEFAULT 'move');

  PROCEDURE drop_tablespace(in_tablespace_name VARCHAR2
                           ,in_including_files INTEGER DEFAULT 1);

  PROCEDURE resize_datafile(in_datafileid INTEGER
                           ,in_new_size INTEGER);

  PROCEDURE create_like_table(in_owner_src VARCHAR2 DEFAULT USER
                             ,in_tab_name_src VARCHAR2
                             ,in_owner_trg VARCHAR2 DEFAULT USER
                             ,in_tab_name_trg VARCHAR2
                             ,in_tablespace_name VARCHAR2
                             ,in_opts VARCHAR2 DEFAULT NULL);

  PROCEDURE execute(in_sql VARCHAR2);

  c_date_format CONSTANT VARCHAR2(8) := 'YYYYMMDD';
  c_datafile_path CONSTANT VARCHAR2(64) := '/path/to/data/files/';
  c_regexp_pattern CONSTANT VARCHAR2 (64) := '^(D|X)([A-Z]+_)([A-Z]+)(_P){0,1}([0-9]+)$';

  partition_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(partition_not_found, -02149);

  partition_value_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(partition_value_exists, -14312);

  partition_bound_inside EXCEPTION;
  PRAGMA EXCEPTION_INIT(partition_bound_inside, -14074);

  table_already_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(table_already_exists, -00955);

  resource_busy EXCEPTION;
  PRAGMA EXCEPTION_INIT(resource_busy, -00054);

  tablespace_already_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(tablespace_already_exists, -01543);

END;
/

CREATE OR REPLACE PACKAGE BODY COMP_SYS.pkg_part_mgmt
AS
  
  FUNCTION partition_exists(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_num INTEGER)
  RETURN BOOLEAN
  IS
    v_exists NUMBER;
  BEGIN

    SELECT 1 
      INTO v_exists
      FROM all_tab_partitions
     WHERE table_name = upper(in_tab_name)
       and table_owner = in_owner
       and partition_name = 'P'||in_num;

    RETURN TRUE;

    EXCEPTION
      WHEN no_data_found
      THEN RETURN FALSE;
  END;

  FUNCTION partition_exists(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE)
  RETURN BOOLEAN
  IS
  BEGIN
    RETURN partition_exists(in_owner => in_owner
                           ,in_tab_name => in_tab_name
                           ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format))); 
  END;

  PROCEDURE truncate_partition(in_owner VARCHAR2 DEFAULT USER
                              ,in_tab_name VARCHAR2
                              ,in_num INTEGER)
  IS 
  BEGIN
    execute( 'ALTER TABLE '
            ||in_owner||'.'||in_tab_name
            ||' TRUNCATE PARTITION '
            ||'P'||TO_CHAR(in_num)
           );
  END;

  PROCEDURE truncate_partition(in_owner VARCHAR2 DEFAULT USER
                              ,in_tab_name VARCHAR2
                              ,in_date_to DATE)
  IS
  BEGIN
    truncate_partition(in_owner => in_owner
                      ,in_tab_name => in_tab_name
                      ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format))); 
  END;

  PROCEDURE add_partition(in_owner VARCHAR2 DEFAULT USER
                         ,in_tab_name VARCHAR2
                         ,in_date_to DATE
                         ,in_type VARCHAR2 DEFAULT 'range'
                         ,in_opts VARCHAR2 DEFAULT NULL)
  IS
  BEGIN
    execute( 'ALTER TABLE '
            ||in_owner||'.'||in_tab_name
            ||' ADD PARTITION '
            ||'P'||TO_CHAR(in_date_to,c_date_format)
            ||' VALUES '
            ||CASE 
               WHEN in_type = 'range'
               THEN 'LESS THAN'
               END
            ||'(TO_DATE('''
            ||CASE 
               WHEN in_type = 'range'
               THEN TO_CHAR(in_date_to+1,c_date_format)
               ELSE TO_CHAR(in_date_to,c_date_format)
               END
            ||''','''
            ||c_date_format
            ||''')) '
            ||in_opts
           );
    EXCEPTION
      WHEN partition_value_exists
      THEN truncate_partition(in_owner => in_owner
                             ,in_tab_name => in_tab_name
                             ,in_date_to => in_date_to);
      WHEN partition_bound_inside
      THEN 
        IF partition_exists(in_owner => in_owner
                           ,in_tab_name => in_tab_name
                           ,in_date_to => in_date_to)
        THEN 
           truncate_partition(in_owner => in_owner
                             ,in_tab_name => in_tab_name
                             ,in_date_to => in_date_to);
        ELSE
          RAISE;
        END IF;
  END;

  PROCEDURE add_partition(in_owner VARCHAR2 DEFAULT USER
                         ,in_tab_name VARCHAR2
                         ,in_num INTEGER
                         ,in_type VARCHAR2 DEFAULT 'range'
                         ,in_opts VARCHAR2 DEFAULT NULL)
  IS 
  BEGIN
    IF in_type = 'range'
    THEN
      execute( 'ALTER TABLE '
              ||in_owner||'.'||in_tab_name
              ||' ADD PARTITION '
              ||'P'||TO_CHAR(in_num)
              ||' VALUES LESS THAN'
              ||'('||TO_CHAR(in_num+1)||') '
              ||in_opts
             );
    ELSE 
      execute( 'ALTER TABLE '
              ||in_owner||'.'||in_tab_name
              ||' ADD PARTITION '
              ||'P'||TO_CHAR(in_num)
              ||' VALUES '
              ||'('||TO_CHAR(in_num)||') '
              ||in_opts
             );
    END IF;
    EXCEPTION
      WHEN partition_value_exists
      THEN truncate_partition(in_owner => in_owner
                             ,in_tab_name => in_tab_name
                             ,in_num => in_num);
      WHEN partition_bound_inside
      THEN 
        IF partition_exists(in_owner => in_owner
                           ,in_tab_name => in_tab_name
                           ,in_num => in_num)
        THEN 
           truncate_partition(in_owner => in_owner
                             ,in_tab_name => in_tab_name
                             ,in_num => in_num);
        ELSE
          RAISE;
        END IF;
  END;

  PROCEDURE drop_partition(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_num INTEGER)
  IS
  BEGIN
    execute( 'ALTER TABLE '
            ||in_owner||'.'||in_tab_name
            ||' DROP PARTITION '
            ||'P'||TO_CHAR(in_num)
           );
    EXCEPTION
      WHEN partition_not_found
      THEN NULL;
  END;

  PROCEDURE drop_partition(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_to DATE)
  IS
  BEGIN
    drop_partition(in_owner => in_owner
                  ,in_tab_name => in_tab_name
                  ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format))); 
  END;

  PROCEDURE create_tablespace(io_tablespace_name IN OUT VARCHAR2)
  IS
  BEGIN
    IF LENGTH(io_tablespace_name)>30
    THEN
      SELECT   prefix
             ||substr(tabname,1,30-length(prefix)-length(postfix))
             ||postfix new_name
        INTO io_tablespace_name
        FROM (SELECT regexp_replace(x,c_regexp_pattern,'\1\2') prefix,
                     regexp_replace(x,c_regexp_pattern,'\3') tabname,
                     regexp_replace(x,c_regexp_pattern,'\4\5') postfix
                FROM (SELECT io_tablespace_name x
                        FROM dual)
             );
    END IF;

    execute(   'CREATE TABLESPACE '
            || io_tablespace_name
            || ' DATAFILE '''
            || c_datafile_path
            || io_tablespace_name
            || '.dbf'''
            || ' SIZE 1M AUTOEXTEND ON');
    EXCEPTION
      WHEN tablespace_already_exists
      THEN NULL;
  END;

  PROCEDURE disable_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_num INTEGER)
  IS
  BEGIN
    FOR rec IN (SELECT i.owner||'.'||i.index_name index_name
                     , ip.partition_name
                  FROM all_ind_partitions ip
                     , all_indexes i
                 WHERE ip.index_owner = in_owner
                   AND i.owner = in_owner
                   AND ip.index_name = i.index_name
                   AND i.table_name = upper(in_tab_name)
                   AND ip.partition_name = 'P'||in_num)
    LOOP
      execute( 'ALTER INDEX '
              ||rec.index_name
              ||' MODIFY PARTITION '
              ||rec.partition_name
              ||' UNUSABLE'
             );
    END LOOP;
  END;

  PROCEDURE disable_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE)
  IS
  BEGIN
    disable_indexes(in_owner => in_owner
                   ,in_tab_name => in_tab_name
                   ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format)));
  END;

  PROCEDURE enable_indexes(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_num INTEGER)
  IS
  BEGIN
    FOR rec IN (SELECT i.owner||'.'||i.index_name index_name
                     , ip.partition_name
                  FROM all_ind_partitions ip
                     , all_indexes i
                 WHERE ip.index_owner = in_owner
                   AND i.owner = in_owner
                   AND ip.index_name = i.index_name
                   AND i.table_name = upper(in_tab_name)
                   AND ip.partition_name = 'P'||in_num)
    LOOP
      execute( 'ALTER INDEX '
              ||rec.index_name
              ||' REBUILD PARTITION '
              ||rec.partition_name
             );
    END LOOP;
  END;  

  PROCEDURE enable_indexes(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_to DATE)
  IS
  BEGIN
    enable_indexes(in_owner => in_owner
                  ,in_tab_name => in_tab_name
                  ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format)));
  END;  

  PROCEDURE rebuild_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_num INTEGER
                           ,in_tablespace_name VARCHAR2 DEFAULT NULL)
  IS
  BEGIN
    FOR rec in (SELECT i.owner||'.'||i.index_name index_name
                     , ip.partition_name
                  FROM all_ind_partitions ip
                     , all_indexes i
                 WHERE ip.index_owner = in_owner
                   AND i.owner = in_owner
                   AND ip.index_name = i.index_name
                   AND i.table_name = upper(in_tab_name)
                   AND ip.partition_name = 'P'||in_num)
    LOOP
      execute( 'ALTER INDEX '
              ||rec.index_name
              ||' REBUILD PARTITION '
              ||rec.partition_name
              ||CASE WHEN in_tablespace_name IS NOT NULL
                     THEN ' TABLESPACE '||in_tablespace_name
                 END
             );
    END LOOP;
  END;

  PROCEDURE rebuild_indexes(in_owner VARCHAR2 DEFAULT USER
                           ,in_tab_name VARCHAR2
                           ,in_date_to DATE
                           ,in_tablespace_name VARCHAR2)
  IS
  BEGIN
    rebuild_indexes(in_owner => in_owner
                   ,in_tab_name => in_tab_name
                   ,in_num => TO_NUMBER(TO_CHAR(in_date_to,c_date_format))
                   ,in_tablespace_name => in_tablespace_name);
  END;

  FUNCTION common_columns(in_owner_src VARCHAR2 DEFAULT USER
                         ,in_tab_name_src VARCHAR2
                         ,in_owner_trg VARCHAR2 DEFAULT USER
                         ,in_tab_name_trg VARCHAR2)
  RETURN columns_type
  IS
    v_cols columns_type;
  BEGIN
    FOR rec IN (SELECT utc1.column_name
                     , utc1.data_type
                  FROM all_tab_columns utc1
                     , all_tab_columns utc2
                 WHERE utc1.owner = upper(in_owner_src)
                   AND utc2.owner = upper(in_owner_trg)
                   AND utc1.table_name = upper(in_tab_name_src)
                   AND utc2.table_name = upper(in_tab_name_trg)
                   AND utc1.column_name = utc2.column_name
                   AND utc1.data_type = utc2.data_type
                 ORDER BY utc1.column_id)
    LOOP
      v_cols.select_cols := v_cols.select_cols 
                          ||CASE
                              WHEN rec.data_type = 'NUMBER'
                               AND rec.column_name not in ('FTP')
                              THEN 'ROUND('||rec.column_name||',4)'
                              ELSE rec.column_name
                             END
                          ||', ';
      v_cols.insert_cols := v_cols.insert_cols
                            ||rec.column_name
                            ||', ';
    END LOOP;
    v_cols.select_cols := RTRIM(v_cols.select_cols, ', ');
    v_cols.insert_cols := RTRIM(v_cols.insert_cols, ', ');
    RETURN v_cols;
  END;

  PROCEDURE make_range(in_owner VARCHAR2 DEFAULT USER
                      ,in_tab_name VARCHAR2
                      ,in_date_from DATE
  	                  ,in_date_to DATE
                      ,in_create_tablespace INTEGER DEFAULT 0)
  IS 
    v_cur_date DATE := in_date_from;
    v_tablespace_name VARCHAR2(255);
  BEGIN

    WHILE v_cur_date <= in_date_to
    LOOP

      BEGIN

  	    truncate_partition(in_owner => in_owner
                          ,in_tab_name => in_tab_name
            	            ,in_date_to => v_cur_date);

        EXCEPTION
          WHEN partition_not_found
          THEN 
               IF in_create_tablespace = 1
               THEN 
                 v_tablespace_name :=  'D'
                                      ||SUBSTR(in_owner,6)
                                      ||'_'
                                      ||REPLACE(in_tab_name,'_','')
                                      ||'_P'
                                      ||TO_CHAR(v_cur_date,c_date_format);
                 create_tablespace(io_tablespace_name => v_tablespace_name);
               END IF;

               add_partition(in_owner => in_owner
                            ,in_tab_name => in_tab_name
                            ,in_date_to => v_cur_date
                            ,in_type => 'range'
                            ,in_opts => CASE WHEN in_create_tablespace = 1
                                             THEN 'TABLESPACE '||v_tablespace_name
                                             ELSE NULL
                                         END);

  	  END;

      v_cur_date := v_cur_date + 1;

    END LOOP;

  END;

  PROCEDURE truncate_range(in_owner VARCHAR2 DEFAULT USER
                          ,in_tab_name VARCHAR2
                          ,in_date_from DATE
                          ,in_date_to DATE)
  IS 
    v_cur_date DATE := in_date_from;
  BEGIN
    WHILE v_cur_date <= in_date_to
    LOOP
      truncate_partition(in_owner => in_owner
                        ,in_tab_name => in_tab_name
                        ,in_date_to => v_cur_date);
      v_cur_date := v_cur_date + 1;
    END LOOP;
  END;

  PROCEDURE move_with_compression(in_owner_src VARCHAR2 DEFAULT USER
                                 ,in_tab_name_src VARCHAR2
                                 ,in_owner_trg VARCHAR2 DEFAULT USER
                                 ,in_tab_name_trg VARCHAR2
                                 ,in_num INTEGER
                                 ,in_mode VARCHAR2 DEFAULT 'move')
  IS 
    v_common_columns columns_type;
  BEGIN

    -- truncate_partition(in_owner => in_owner_trg
    --                   ,in_tab_name => in_tab_name_trg
    --                   ,in_num => in_num);

    v_common_columns := common_columns(in_owner_src => in_owner_src
                                      ,in_tab_name_src => in_tab_name_src
                                      ,in_owner_trg => in_owner_trg
                                      ,in_tab_name_trg => in_tab_name_trg);

    execute( 'INSERT /*+ APPEND */ INTO '
            ||in_owner_trg||'.'||in_tab_name_trg
            ||'('||v_common_columns.insert_cols||')'
            ||' SELECT '||v_common_columns.select_cols
            ||' FROM '
            ||in_owner_src||'.'||in_tab_name_src
           );

    -- rebuild_indexes(in_owner => in_owner_trg
    --                ,in_tab_name => in_tab_name_trg
    --                ,in_num => in_num);

    IF in_mode = 'move'
    THEN 
      execute( 'DROP TABLE '
              ||in_owner_src||'.'||in_tab_name_src
              ||' PURGE');
    END IF;



  END;

  PROCEDURE drop_tablespace(in_tablespace_name VARCHAR2
                           ,in_including_files INTEGER DEFAULT 1)
  IS
  BEGIN
    IF in_including_files = 1
    THEN
      FOR rec IN (SELECT file_id
                    FROM dba_data_files d
                   WHERE d.tablespace_name = in_tablespace_name)
        LOOP
           resize_datafile (in_datafileid => rec.file_id
                           ,in_new_size => 2097152);
        END LOOP;
    END IF;
    execute( 'DROP TABLESPACE '
            ||in_tablespace_name
            ||CASE WHEN in_including_files=1
                   THEN ' INCLUDING CONTENTS AND DATAFILES'
               END
           );
  END;
 
  PROCEDURE resize_datafile(in_datafileid INTEGER
  	                       ,in_new_size INTEGER)
  IS 
  BEGIN
    pkg_developer_dba_work.resize_datafile(v_datafileid => in_datafileid
                                          ,size_bytes => in_new_size
                                          );
  END;

  PROCEDURE create_like_table(in_owner_src VARCHAR2 DEFAULT USER
                             ,in_tab_name_src VARCHAR2
                             ,in_owner_trg VARCHAR2 DEFAULT USER
                             ,in_tab_name_trg VARCHAR2
                             ,in_tablespace_name VARCHAR2
                             ,in_opts VARCHAR2 DEFAULT NULL)
  IS
  BEGIN
    execute( 'CREATE TABLE '
            ||in_owner_trg||'.'||in_tab_name_trg
            ||' TABLESPACE '||in_tablespace_name
            ||in_opts
            ||' AS SELECT * FROM '
            ||in_owner_src||'.'||in_tab_name_src
            ||' WHERE 1=2');
    EXCEPTION
      WHEN table_already_exists
      THEN execute('TRUNCATE TABLE '||in_owner_trg||'.'||in_tab_name_trg);
  END;

  PROCEDURE execute(in_sql VARCHAR2)
  IS 
    v_log_uk NUMBER;
    v_sql VARCHAR2(4000);
  BEGIN

    v_sql := TRIM(in_sql);

    pkg_log.new_entry(in_sql, v_log_uk);

    BEGIN
      EXECUTE IMMEDIATE v_sql;
      EXCEPTION
        WHEN others
        THEN pkg_log.error_entry(v_log_uk
                                ,v_sql
                                ,SQLCODE
                                ,SQLERRM);
             RAISE;
    END;

  END;


END;
/
