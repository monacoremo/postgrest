-- TODOs
-- * why does columns need distinct?


with
  -- Postgres version

  pg_version as (
    select
      current_setting('server_version_num')::integer as pgv_num,
      current_setting('server_version') as pgv_name
  ),


  -- Schema descriptions

  schemas as (
    select
      n.oid,
      n.nspname as schema_name,
      description as schema_description
    from
      pg_namespace n
      left join pg_description d on d.objoid = n.oid
    where
      n.nspname = any ($1)
  ),


  -- Procedures

  procs as (
    select
      pn.nspname as proc_schema,
      p.proname as proc_name,
      d.description as proc_description,
      json_build_object(
        'qi_schema', tn.nspname,
        'qi_name', coalesce(comp.relname, t.typname)
      ) as proc_return_type_qi,
      p.proretset as proc_return_type_is_setof,
      case t.typtype
        when 'c' then true
        when 'p' then coalesce(comp.relname, t.typname) = 'record' -- Only pg pseudo type that is a row type is 'record'
        else false -- 'b'ase, 'd'omain, 'e'num, 'r'ange
      end as proc_return_type_is_composite,
      p.provolatile as proc_volatility,
      has_function_privilege(p.oid, 'execute') as proc_is_accessible,
      coalesce(array(
        select
          json_build_object(
            'pga_name', parsed.name,
            'pga_type', parsed.typ,
            'pga_req', not parsed.has_default
          )
        from
          regexp_split_to_table(pg_get_function_arguments(p.oid), ', ') as args
          , regexp_split_to_array(args, ' DEFAULT ') as args_with_default
          , regexp_matches(
              args_with_default[1],
              '^(IN |INOUT |OUT |)(([^\" ]\S*?)|\"(\S+?)\")( (.+?))?$'
          ) as groups
          , lateral (
              select
                groups[1] as inout,
                coalesce(groups[3], groups[4]) as name,
                coalesce(groups[6], '') as typ,
                args_with_default[2] is not null as has_default
          ) as parsed
        where parsed.inout <> 'OUT '
      ), array[]::json[]) as proc_args
    from
      pg_proc p
      join pg_namespace pn on pn.oid = p.pronamespace
      join pg_type t on t.oid = p.prorettype
      join pg_namespace tn on tn.oid = t.typnamespace
      left join pg_class comp on comp.oid = t.typrelid
      left join pg_description as d on d.objoid = p.oid
    where
      pn.nspname = any ($1)
  ),


  -- Tables

  tables as (
    select
      c.oid,
      n.nspname as table_schema,
      c.relname as table_name,
      d.description as table_description,
      (
        c.relkind in ('r', 'v', 'f')
        and (pg_relation_is_updatable(c.oid::regclass, false) & 8) = 8
        -- The function `pg_relation_is_updateable` returns a bitmask where 8
        -- corresponds to `1 << CMD_INSERT` in the PostgreSQL source code, i.e.
        -- it's possible to insert into the relation.
        or exists (
          select 1
          from pg_trigger
          where
            pg_trigger.tgrelid = c.oid
            and (pg_trigger.tgtype::integer & 69) = 69
            -- The trigger type `tgtype` is a bitmask where 69 corresponds to
            -- TRIGGER_TYPE_ROW + TRIGGER_TYPE_INSTEAD + TRIGGER_TYPE_INSERT
            -- in the PostgreSQL source code.
        )
      ) as table_insertable,
      (
        pg_has_role(c.relowner, 'USAGE')
        or has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
        or has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')
      ) as table_is_accessible
    from
      pg_class c
      join pg_namespace n
        on n.oid = c.relnamespace
      left join pg_catalog.pg_description as d
        on d.objoid = c.oid and d.objsubid = 0
    where
      c.relkind IN ('v','r','m','f')
      and n.nspname NOT IN ('pg_catalog', 'information_schema')
    order by
      table_schema, table_name
  ),


  -- Columns

  columns as (
    select distinct
      col_table,
      a.attrelid as col_table_oid,
      nc.nspname as col_schema,
      c.relname as col_table_name,
      a.attname as col_name,
      d.description as col_description,
      a.attnum as col_position,
      pg_get_expr(ad.adbin, ad.adrelid)::text as col_default,
      not (a.attnotnull or t.typtype = 'd' and t.typnotnull) as col_nullable,
      case
        when t.typtype = 'd' then
          case
            when bt.typelem <> 0::oid and bt.typlen = (-1) then
              'ARRAY'::text
            when nbt.nspname = 'pg_catalog'::name then
              format_type(t.typbasetype, null::integer)
            else
              format_type(a.atttypid, a.atttypmod)
          end
        else
          case
            when t.typelem <> 0::oid and t.typlen = (-1) then
              'ARRAY'
            when nt.nspname = 'pg_catalog'::name then
              format_type(a.atttypid, null::integer)
            else
              format_type(a.atttypid, a.atttypmod)
          end
      end as col_type,
      information_schema._pg_char_max_length(truetypid, truetypmod) as col_max_len,
      information_schema._pg_numeric_precision(truetypid, truetypmod) as col_precision,
      (
        c.relkind in ('r', 'v', 'f')
        and pg_column_is_updatable(c.oid::regclass, a.attnum, false)
      ) col_updatable,
      coalesce(enum_info.vals, array[]::text[]) as col_enum
    from
      pg_attribute a
      left join pg_description d on d.objoid = a.attrelid and d.objsubid = a.attnum
      left join pg_attrdef ad on a.attrelid = ad.adrelid and a.attnum = ad.adnum
      join pg_class c on a.attrelid = c.oid
      join pg_namespace nc on c.relnamespace = nc.oid
      join pg_type t on a.atttypid = t.oid
      join pg_namespace nt on t.typnamespace = nt.oid
      left join pg_type bt on t.typtype = 'd' and t.typbasetype = bt.oid
      left join pg_namespace nbt on bt.typnamespace = nbt.oid
      join tables col_table on col_table.oid = a.attrelid
      , lateral (
          select array_agg(e.enumlabel order by e.enumsortorder) as vals
          from
            pg_type et
            join pg_enum e on et.oid = e.enumtypid
          where
            et.oid = t.oid
      ) as enum_info
      , information_schema._pg_truetypid(a.*, t.*) truetypid
      , information_schema._pg_truetypmod(a.*, t.*) truetypmod
    where
      not pg_is_other_temp_schema(nc.oid)
      and a.attnum > 0
      and not a.attisdropped
      and c.relkind in ('r', 'v', 'f', 'm')
      and (
        nc.nspname = any ($1)
        or exists(
            select 1
            from
              pg_constraint r
            where
              -- can we keep only ones that point to our schema?
              r.conrelid = a.attrelid
              and a.attnum = any(r.conkey)
              and r.contype in ('f', 'p', 'u')
        )
      )
    order by col_schema, col_position
  ),


  -- M2O relations

  table_table_m2o_rels as (
     select
        conname as rel_constraint,
        rel_table,
        rel_f_table,
        rel_table.oid as rel_table_oid,
        rel_f_table.oid as rel_f_table_oid,
        array(
          select columns
          from columns
          where
            col_table_oid = conrelid
            and col_position = any (conkey)
        ) as rel_columns,
        conkey as rel_column_positions,
        array(
          select columns
          from columns
          where
            col_table_oid = confrelid
            and col_position = any (confkey)
        ) as rel_f_columns,
        confkey as rel_f_column_positions,
        'M2O' as rel_type
     from
       pg_constraint
       join tables rel_table on rel_table.oid = conrelid
       join tables rel_f_table on rel_f_table.oid = confrelid
     where confrelid != 0
     order by conrelid, conkey
  ),



  -- Source columns

  -- query explanation at https://gist.github.com/steve-chavez/7ee0e6590cddafb532e5f00c46275569

  view_table_columns as (
    select
      c.oid as view_oid,
      substring(entry from ':resname (.*?) :') as view_col_name,
      substring(entry from ':resorigtbl (.*?) :')::oid as table_oid,
      substring(entry from ':resorigcol (.*?) :')::integer as col_position
    from
      pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      join pg_rewrite r on r.ev_class = c.oid
      , regexp_replace(
          r.ev_action,
          -- "result" appears when the subselect is used inside "case when", see
          -- `authors_have_book_in_decade` fixture
          -- "resno"  appears in every other case
          case when (select pgv_num from pg_version) < 100000 then
            ':subselect {.*?:constraintDeps <>} :location \d+} :res(no|ult)'
          else
            ':subselect {.*?:stmt_len 0} :location \d+} :res(no|ult)'
          end,
          '',
          'g'
        ) as x
      , regexp_split_to_array(x, 'targetList') as target_lists
      , lateral (
          select
            (regexp_split_to_array(
              target_lists[array_upper(target_lists, 1)], ':onConflict'
            ))[1]
        ) last_target_list_wo_tail
      , unnest(regexp_split_to_array(last_target_list_wo_tail::text, 'TARGETENTRY')) as entry
    where
      c.relkind in ('v', 'm')
      and n.nspname = ANY ($1)
  ),

  -- Primary keys

  table_primary_keys as (
    select
      a.attname as pk_name,
      pk_table
    from
      pg_constraint c
      join pg_class r on r.oid = c.conrelid
      join pg_namespace nr on nr.oid = r.relnamespace
      join tables pk_table on pk_table.oid = c.conrelid
      join pg_attribute a on r.oid = a.attrelid
      , information_schema._pg_expandarray(c.conkey) as x
    where
      c.contype = 'p'
      and r.relkind = 'r'
      and nr.nspname not in ('pg_catalog', 'information_schema')
      and not pg_is_other_temp_schema(nr.oid)
      and a.attnum = x.x
      and not a.attisdropped
  ),

  view_primary_keys as (
    select
      view_col_name as pk_name,
      pk_table
    from
      table_primary_keys pks
      join view_table_columns view_cols
        on view_cols.table_oid = (pks.pk_table).oid
      join tables pk_table
        on pk_table.oid = view_cols.view_oid
  ),

  primary_keys as (
    select * from table_primary_keys
    union all
    select * from view_primary_keys
  ),

  table_views as (
    select
      table_oid,
      view_oid,
      array_agg(col_pairs) as col_pais
    from
      view_table_columns cols
      join columns table_col
        on
          table_col.col_table_oid = cols.table_oid
          and table_col.col_position = cols.col_position
      join columns view_col
        on
          view_col.col_table_oid = cols.view_oid
          and view_col.col_name = cols.view_col_name
      , lateral (select table_col, view_col) as col_pairs
    group by
      cols.view_oid, cols.table_oid
  ),

  -- Many to one relations from views to tables
  view_table_m2o_rels as (
     select
        rels.rel_constraint,
        'M2O' as rel_type,
        rels.rel_f_table,
        rels.rel_f_columns,

        -- replace the rel_table with each view that refers to the rel_f_table
        rel_view as rel_table,
        array(
          select
            view_cols
          from
            unnest(rels.rel_f_column_positions) as pos
            join view_table_columns cols
              on
                cols.table_oid = rels.rel_f_table_oid
                and cols.col_position = pos
            join columns view_cols
              on view_cols.col_table_oid = rel_view.oid
              and view_cols.col_name = cols.view_col_name
        ) as rel_columns
    from
       table_table_m2o_rels rels
       join table_views on rels.rel_f_table_oid = table_views.table_oid
       join tables rel_view on rel_view.oid = table_views.view_oid
  ),

  table_view_m2o_rels as (
     select
        rels.rel_constraint,
        'M2O' as rel_type,
        rels.rel_table,
        rels.rel_columns,

        -- replace the rel_table with each view that refers to the rel_f_table
        rel_f_view as rel_f_table,
        array(
          select
            view_cols
          from
            unnest(rels.rel_column_positions) as pos
            join view_table_columns cols
              on
                cols.table_oid = rels.rel_table_oid
                and cols.col_position = pos
            join columns view_cols
              on view_cols.col_table_oid = rel_f_view.oid
              and view_cols.col_name = cols.view_col_name
        ) as rel_f_columns
    from
       table_table_m2o_rels rels
       join table_views on rels.rel_table_oid = table_views.table_oid
       join tables rel_f_view on rel_f_view.oid = table_views.view_oid
  ),

  view_view_m2o_rels as (
     select
        rels.rel_constraint,
        'M2O' as rel_type,

        rel_view as rel_table,
        array(
          select
            view_cols
          from
            unnest(rels.rel_column_positions) as pos
            join view_table_columns cols
              on
                cols.table_oid = rels.rel_table_oid
                and cols.col_position = pos
            join columns view_cols
              on view_cols.col_table_oid = rel_view.oid
              and view_cols.col_name = cols.view_col_name
        ) as rel_columns,

        rel_f_view as rel_f_table,
        array(
          select
            view_cols
          from
            unnest(rels.rel_f_column_positions) as pos
            join view_table_columns cols
              on
                cols.table_oid = rels.rel_table_oid
                and cols.col_position = pos
            join columns view_cols
              on view_cols.col_table_oid = rel_f_view.oid
              and view_cols.col_name = cols.view_col_name
        ) as rel_f_columns
    from
       table_table_m2o_rels rels
       join table_views on rels.rel_table_oid = table_views.table_oid
       join tables rel_view on rel_view.oid = table_views.view_oid
       join table_views table_f_views on rels.rel_f_table_oid = table_f_views.table_oid
       join tables rel_f_view on rel_f_view.oid = table_views.view_oid
  ),

  m2o_rels as (
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[]
    from table_table_m2o_rels
    union all
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[]
    from table_view_m2o_rels
    union all
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[]
    from view_table_m2o_rels
    union all
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[]
    from table_view_m2o_rels
  ),

  o2m_rels as (
    select
      rel_constraint,
      'O2M' as rel_type,
      m2o_rels.rel_f_table as rel_table,
      m2o_rels.rel_f_columns as rel_columns,
      m2o_rels.rel_table as rel_f_table,
      m2o_rels.rel_columns as rel_f_columns
    from
      m2o_rels
  ),

  m2m_rels as (
    select
      'M2M' as rel_type,
      left_rels.rel_f_table as rel_table,
      left_rels.rel_f_columns as rel_columns,
      right_rels.rel_f_table as rel_f_table,
      right_rels.rel_f_columns as rel_f_columns,
      json_build_object(
        'jun_table', left_rels.rel_table,
        'jun_constraint1', left_rels.rel_constraint,
        'jun_cols1', left_rels.rel_columns,
        'jun_constraint2', right_rels.rel_constraint,
        'jun_cols2', right_rels.rel_columns
      ) as rel_junction
    from
      -- all combinations of rels that have the same rel_table
      m2o_rels left_rels
      , m2o_rels right_rels
    where
      (right_rels.rel_table).oid = (left_rels.rel_table).oid
      and right_rels.rel_columns <> left_rels.rel_columns
  ),

  rels as (
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[],
      null::json as rel_junction
    from m2o_rels
    union all
    select
      rel_constraint::text,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[],
      null::json as rel_junction
    from o2m_rels
    union all
    select
      null::text as rel_constraint,
      rel_type::text,
      rel_table::record,
      rel_columns::record[],
      rel_f_table::record,
      rel_f_columns::record[],
      rel_junction::json
    from m2m_rels
  )

  -- Main query

  select
    json_build_object(
      'raw_db_procs', coalesce(procs_agg.array_agg, array[]::record[]),
      'raw_db_schemas', coalesce(schemas_agg.array_agg, array[]::record[]),
      'raw_db_tables', coalesce(tables_agg.array_agg, array[]::record[]),
      'raw_db_columns', coalesce(columns_agg.array_agg, array[]::record[]),
      'raw_db_rels', coalesce(rels_agg.array_agg, array[]::record[]),
      'raw_db_primary_keys', coalesce(primary_keys_agg.array_agg, array[]::record[]),
      'raw_db_pg_ver', pg_version
    ) as dbstructure
  from
    (select array_agg(procs) from procs) procs_agg,
    (select array_agg(schemas) from schemas) schemas_agg,
    (select array_agg(tables) from tables) as tables_agg,
    (select array_agg(columns) from columns) as columns_agg,
    (select array_agg(rels) from rels) as rels_agg,
    (select array_agg(primary_keys) from primary_keys) as primary_keys_agg,
    pg_version
