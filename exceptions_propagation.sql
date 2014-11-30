

drop table t;
create table t (id number primary key, name varchar2(10));


declare
   procedure log_error(message_in in varchar2)
   is
   begin
      dbms_output.put_line(message_in);
   end;

   procedure count_rows
   is
      l_count number;
   begin
      select count(*)
        into l_count
        from t;
      dbms_output.put_line(l_count);
   end;

   procedure make_inserts
   is
   begin
      insert into t values(1, 'a');
      insert into t values(2, 'b');
      insert into t values(3, 'c');
      insert into t values(1, 'a');
   exception
      when others
      then log_error('error at make_inserts');
           raise;
   end;
begin
   count_rows();
   make_inserts();
   count_rows();
exception
   when others
   then log_error('error at top level');
end;
/

rollback;

SELECT * FROM t;


