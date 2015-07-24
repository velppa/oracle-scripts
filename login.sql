col user format a30
col instance_name format a30

select user
     , sys_context('userenv', 'instance_name') instance_name
  from dual
;
