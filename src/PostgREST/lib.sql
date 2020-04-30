-- Make sure that we can create temporary functions if we are in a transaction.
set transaction read write;


-- Numeric precision

-- Returns the numeric precision of the given column, if applicable.

create or replace function pg_temp.postgrest_numeric_precision(a pg_attribute, t pg_type)
  returns integer
  language sql
  as $$
    select
      information_schema._pg_char_max_length(
        information_schema._pg_truetypid(a.*, t.*),
        information_schema._pg_truetypmod(a.*, t.*)
      )
  $$;


-- Tables

create or replace view pg_temp.postgrest_tables as
  select
    n.nspname AS table_schema,
    c.relname AS table_name,
    d.description as table_description,
    (
      c.relkind in ('r', 'v', 'f')
      and pg_relation_is_updatable(c.oid::regclass, false) & 8 = 8
      -- The function `pg_relation_is_updateable` returns a bitmask where 8
      -- corresponds to `1 << CMD_INSERT` in the PostgreSQL source code, i.e.
      -- it's possible to insert into the relation.
      or exists (
        select 1
        from pg_trigger
        where
          pg_trigger.tgrelid = c.oid
          and pg_trigger.tgtype::integer & 69 = 69
          -- The trigger type `tgtype` is a bitmask where 69 corresponds to
          -- TRIGGER_TYPE_ROW + TRIGGER_TYPE_INSTEAD + TRIGGER_TYPE_INSERT
          -- in the PostgreSQL source code.
      )
    ) as insertable,
    (
      pg_has_role(c.relowner, 'USAGE')
      or has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
      or has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')
    ) as is_accessible
  from
    pg_class c
    join pg_namespace n
      on n.oid = c.relnamespace
    left join pg_catalog.pg_description d
      on d.objoid = c.oid and d.objsubid = 0
  where
    c.relkind in ('v','r','m','f')
    and n.nspname not in ('pg_catalog', 'information_schema')
  order by
    table_schema, table_name;
