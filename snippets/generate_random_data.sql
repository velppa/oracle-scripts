
-- source: https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:2151576678914

create or replace procedure clone( p_tname in varchar2, p_records in number )
  authid current_user
as
   l_insert long;
   l_rows   number default 0;
begin
  execute immediate 'create table clone_' || p_tname ||
                    ' as select * from ' || p_tname ||
                    ' where 1=0';
   l_insert := 'insert into clone_' || p_tname ||
               ' select ';
   for x in ( select data_type, data_length,
               rpad( '9',data_precision,'9')/power(10,data_scale) maxval
                from user_tab_columns
               where table_name = 'CLONE_' || upper(p_tname)
               order by column_id )
   loop
       if ( x.data_type in ('NUMBER', 'FLOAT' ))
       then
           l_insert := l_insert || 'dbms_random.value(1,' || x.maxval ||
                                                                     '),';
       elsif ( x.data_type = 'DATE' )
       then
           l_insert := l_insert || 
                 'sysdate+dbms_random.value+dbms_random.value(1,1000),';
       else
           l_insert := l_insert || 'dbms_random.string(''A'',' || 
                                      x.data_length || '),';
       end if;
   end loop;
   l_insert := rtrim(l_insert,',') || 
                 ' from all_objects where rownum <= :n';
   loop
       execute immediate l_insert using p_records - l_rows;
       l_rows := l_rows + sql%rowcount;
       exit when ( l_rows >= p_records );
   end loop;
end;
/
