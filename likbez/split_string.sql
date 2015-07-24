with test as 
   (select 'SOME,COMMA,DELIMITED,STRING' str
      from dual)
select regexp_substr (str, '[^,]+', 1, rownum) split  
  from test  
connect by level <= regexp_count(str, '[^,]+');
