with


  -- Postgres version

  pg_version as (
    SELECT
      current_setting('server_version_num')::integer as pgv_num,
      current_setting('server_version') as pgv_name
  ),


  -- Procedures

  procs as (
    select
      pn.nspname as proc_schema,
      p.proname as proc_name,
      d.description as proc_description,
      pg_get_function_arguments(p.oid) as proc_args,
      tn.nspname as proc_return_type_schema,
      coalesce(comp.relname, t.typname) as proc_return_type_name,
      p.proretset as proc_return_type_is_setof,
      t.typtype as proc_return_type,
      p.provolatile as proc_volatility,
      has_function_privilege(p.oid, 'execute') as proc_is_accessible
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


  -- Schema description

  schemas as (
    select
      n.nspname as schema_name,
      description as schema_description
    from
      pg_namespace n
      left join pg_description d on d.objoid = n.oid
    where
      n.nspname = any ($1)
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
     SELECT DISTINCT
         col_table as col_table,
         info.table_oid as col_table_oid,
         info.column_name AS col_name,
         info.table_schema AS col_schema,
         info.table_name as col_table_name,
         info.description AS col_description,
         info.ordinal_position AS col_position,
         info.is_nullable::boolean AS col_nullable,
         info.data_type AS col_type,
         info.is_updatable::boolean AS col_updatable,
         info.character_maximum_length AS col_max_len,
         info.numeric_precision AS col_precision,
         info.column_default AS col_default,
         coalesce(enum_info.vals, array[]::text[]) AS col_enum
     FROM (
         -- CTE based on pg_catalog to get PRIMARY/FOREIGN key and UNIQUE columns outside api schema
         WITH key_columns AS (
              SELECT
                r.oid AS r_oid,
                c.oid AS c_oid,
                n.nspname,
                c.relname,
                r.conname,
                r.contype,
                unnest(r.conkey) AS conkey
              FROM
                pg_catalog.pg_constraint r,
                pg_catalog.pg_class c,
                pg_catalog.pg_namespace n
              WHERE
                r.contype IN ('f', 'p', 'u')
                AND c.relkind IN ('r', 'v', 'f', 'm')
                AND r.conrelid = c.oid
                AND c.relnamespace = n.oid
                AND n.nspname <> ANY (ARRAY['pg_catalog', 'information_schema'] || $1)
         ),
         -- CTE based on information_schema.columns
         columns AS (
             SELECT
                 a.attrelid as table_oid,
                 nc.nspname AS table_schema,
                 c.relname AS table_name,
                 a.attname AS column_name,
                 d.description AS description,
                 a.attnum AS ordinal_position,
                 pg_get_expr(ad.adbin, ad.adrelid)::text AS column_default,
                 not (a.attnotnull OR t.typtype = 'd' AND t.typnotnull) AS is_nullable,
                     CASE
                         WHEN t.typtype = 'd' THEN
                         CASE
                             WHEN bt.typelem <> 0::oid AND bt.typlen = (-1) THEN 'ARRAY'::text
                             WHEN nbt.nspname = 'pg_catalog'::name THEN format_type(t.typbasetype, NULL::integer)
                             ELSE format_type(a.atttypid, a.atttypmod)
                         END
                         ELSE
                         CASE
                             WHEN t.typelem <> 0::oid AND t.typlen = (-1) THEN 'ARRAY'::text
                             WHEN nt.nspname = 'pg_catalog'::name THEN format_type(a.atttypid, NULL::integer)
                             ELSE format_type(a.atttypid, a.atttypmod)
                         END
                     END::text AS data_type,
                 information_schema._pg_char_max_length(
                     information_schema._pg_truetypid(a.*, t.*),
                     information_schema._pg_truetypmod(a.*, t.*)
                 )::integer AS character_maximum_length,
                 information_schema._pg_numeric_precision(
                     information_schema._pg_truetypid(a.*, t.*),
                     information_schema._pg_truetypmod(a.*, t.*)
                 )::integer AS numeric_precision,
                 COALESCE(bt.typname, t.typname)::name AS udt_name,
                 (
                     c.relkind in ('r', 'v', 'f')
                     AND pg_column_is_updatable(c.oid::regclass, a.attnum, false)
                 )::bool is_updatable
             FROM pg_attribute a
                 LEFT JOIN key_columns kc
                     ON kc.conkey = a.attnum AND kc.c_oid = a.attrelid
                 LEFT JOIN pg_catalog.pg_description AS d
                     ON d.objoid = a.attrelid and d.objsubid = a.attnum
                 LEFT JOIN pg_attrdef ad
                     ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                 JOIN (pg_class c JOIN pg_namespace nc ON c.relnamespace = nc.oid)
                     ON a.attrelid = c.oid
                 JOIN (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid)
                     ON a.atttypid = t.oid
                 LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)
                     ON t.typtype = 'd' AND t.typbasetype = bt.oid
                 LEFT JOIN (pg_collation co JOIN pg_namespace nco ON co.collnamespace = nco.oid)
                     ON a.attcollation = co.oid AND (nco.nspname <> 'pg_catalog'::name OR co.collname <> 'default'::name)
             WHERE
                 NOT pg_is_other_temp_schema(nc.oid)
                 AND a.attnum > 0
                 AND NOT a.attisdropped
                 AND c.relkind in ('r', 'v', 'f', 'm')
                 -- Filter only columns that are FK/PK or in the api schema:
                 AND (nc.nspname = ANY ($1) OR kc.r_oid IS NOT NULL)
         )
         SELECT
             table_oid,
             table_schema,
             table_name,
             column_name,
             description,
             ordinal_position,
             is_nullable,
             data_type,
             is_updatable,
             character_maximum_length,
             numeric_precision,
             column_default,
             udt_name
         FROM columns
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
     ) AS info
     LEFT OUTER JOIN (
         SELECT
             n.nspname AS s,
             t.typname AS n,
             array_agg(e.enumlabel ORDER BY e.enumsortorder) AS vals
         FROM pg_type t
         JOIN pg_enum e ON t.oid = e.enumtypid
         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
         GROUP BY s,n
     ) AS enum_info ON (info.udt_name = enum_info.n)
     , lateral (
        select
          -- explicit columns needed for Postgres < 10
          table_schema::text,
          table_name::text,
          table_description::text,
          table_insertable::bool,
          table_is_accessible::bool
        from tables
        where
          tables.table_schema::text = info.table_schema::text
          and tables.table_name::text = info.table_name::text
     ) col_table
     ORDER BY col_schema, col_position
  ),


  -- M2O relations

  m2o_rels as (
     select
        conname as rel_constraint,
        rel_table,
        rel_f_table,
        array(
          select columns
          from columns
          where
            col_table_oid = conrelid
            and col_position = any (conkey)
        ) as rel_columns,
        array(
          select columns
          from columns
          where
            col_table_oid = confrelid
            and col_position = any (confkey)
        ) as rel_f_columns,
        'M2O' as rel_type
     from
       pg_constraint
       join tables rel_table on rel_table.oid = conrelid
       join tables rel_f_table on rel_f_table.oid = confrelid
     where confrelid != 0
     order by conrelid, conkey
  ),


  -- Primary keys

  primary_keys as (
     WITH tc AS (
         SELECT
             c.conname AS constraint_name,
             nr.nspname AS table_schema,
             r.relname AS table_name
         FROM pg_namespace nc,
             pg_namespace nr,
             pg_constraint c,
             pg_class r
         WHERE
             nr.nspname NOT IN ('pg_catalog', 'information_schema')
             and nc.oid = c.connamespace
             AND nr.oid = r.relnamespace
             AND c.conrelid = r.oid
             AND r.relkind = 'r'
             AND NOT pg_is_other_temp_schema(nr.oid)
             AND c.contype = 'p'
     ),
     kc AS (
         SELECT
             ss.conname AS constraint_name,
             ss.nr_nspname AS table_schema,
             ss.relname AS table_name,
             a.attname AS column_name,
             (ss.x).n AS ordinal_position,
             CASE
                 WHEN ss.contype = 'f'
                 THEN information_schema._pg_index_position(ss.conindid, ss.confkey[(ss.x).n])
                 ELSE null
             END::integer AS position_in_unique_constraint
         FROM pg_attribute a,
             ( SELECT r.oid AS roid,
                 r.relname,
                 r.relowner,
                 nc.nspname AS nc_nspname,
                 nr.nspname AS nr_nspname,
                 c.oid AS coid,
                 c.conname,
                 c.contype,
                 c.conindid,
                 c.confkey,
                 information_schema._pg_expandarray(c.conkey) AS x
                FROM pg_namespace nr,
                 pg_class r,
                 pg_namespace nc,
                 pg_constraint c
               WHERE
                 nr.oid = r.relnamespace
                 AND r.oid = c.conrelid
                 AND nc.oid = c.connamespace
                 AND c.contype in ('p', 'u', 'f')
                 AND r.relkind = 'r'
                 AND NOT pg_is_other_temp_schema(nr.oid)
             ) ss
         WHERE
           ss.roid = a.attrelid
           AND a.attnum = (ss.x).x
           AND NOT a.attisdropped
     )
     select
         kc.table_schema,
         kc.table_name,
         kc.column_name as pk_name,
         pk_table
     from
         tc
         join kc
           on
             kc.table_name = tc.table_name
             and kc.table_schema = tc.table_schema
             and kc.constraint_name = tc.constraint_name
         join tables pk_table
           on
              pk_table.table_schema::text = kc.table_schema::text
              and pk_table.table_name::text = kc.table_name::text
  ),


  -- Source columns

  -- query explanation at https://gist.github.com/steve-chavez/7ee0e6590cddafb532e5f00c46275569

  source_columns as (
    with
      entries as (
        select
          n.nspname as view_schema,
          c.relname as view_name,
          r.ev_action as view_definition,
          last_target_list_wo_tail::text as x,
          substring(entry from ':resname (.*?) :') as view_colum_name,
          substring(entry from ':resorigtbl (.*?) :') as resorigtbl,
          substring(entry from ':resorigcol (.*?) :') as resorigcol
        from
          pg_class c
          join pg_namespace n on n.oid = c.relnamespace
          join pg_rewrite r on r.ev_class = c.oid
          , regexp_replace(
              r.ev_action,
              -- "result" appears when the subselect is used inside "case when", see
              -- `authors_have_book_in_decade` fixture
              -- "resno"  appears in every other case
              case when (select pgv_num from pg_version) < 100000
                 then ':subselect {.*?:constraintDeps <>} :location \d+} :res(no|ult)'
                 else ':subselect {.*?:stmt_len 0} :location \d+} :res(no|ult)'
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
      )
      select
        sch.nspname as table_schema,
        tbl.relname as table_name,
        col.attname as table_column_name,
        res.view_schema,
        res.view_name,
        res.view_colum_name,
        source_column as src_source,
        view_column as src_view
      from
        entries res
        join pg_class tbl
          on tbl.oid::text = res.resorigtbl
        join pg_attribute col
          on
            col.attrelid = tbl.oid
            and col.attnum::text = res.resorigcol
        join pg_namespace sch
          on sch.oid = tbl.relnamespace
        join columns source_column
          on
            sch.nspname = source_column.col_schema
            and tbl.relname = source_column.col_table_name
            and col.attname = source_column.col_name
        join columns view_column
          on
            res.view_schema = view_column.col_schema
            and res.view_name = view_column.col_table_name
            and res.view_colum_name = view_column.col_name
        where
          resorigtbl <> '0'
        order by
          view_schema, view_name, view_colum_name
  )


  -- Main query

  select
    json_build_object(
      'raw_db_procs', coalesce(procs_agg.array_agg, array[]::record[]),
      'raw_db_schemas', coalesce(schemas_agg.array_agg, array[]::record[]),
      'raw_db_tables', coalesce(tables_agg.array_agg, array[]::record[]),
      'raw_db_columns', coalesce(columns_agg.array_agg, array[]::record[]),
      'raw_db_m2o_rels', coalesce(m2o_rels_agg.array_agg, array[]::record[]),
      'raw_db_primary_keys', coalesce(primary_keys_agg.array_agg, array[]::record[]),
      'raw_db_source_columns', coalesce(source_columns_agg.array_agg, array[]::record[]),
      'raw_db_pg_ver', pg_version
    ) as dbstructure
  from
    (select array_agg(procs) from procs) procs_agg,
    (select array_agg(schemas) from schemas) schemas_agg,
    (select array_agg(tables) from tables) as tables_agg,
    (select array_agg(columns) from columns) as columns_agg,
    (select array_agg(m2o_rels) from m2o_rels) as m2o_rels_agg,
    (select array_agg(primary_keys) from primary_keys) as primary_keys_agg,
    (select array_agg(source_columns) from source_columns) as source_columns_agg,
    pg_version
