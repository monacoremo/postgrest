select vals
from
  regexp_split_to_table(
      'INOUT double double precision, IN "varchar" character varying, OUT "boolean" boolean, date date, money money, enum test.enum_menagerie_type, "integer" integer DEFAULT 42, json json DEFAULT ''{}''::json, jsonb jsonb DEFAULT ''{}''::jsonb',
      ', '
    ) as args
  , regexp_matches(
      args,
      '^(IN |INOUT |OUT |)([^\"]\S*[^\"]|\"(\S*)\")( (.*?))?( DEFAULT .*?)?$'
    ) as groups
  , unnest(groups) as vals;
