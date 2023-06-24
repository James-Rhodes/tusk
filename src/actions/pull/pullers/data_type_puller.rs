use crate::actions::pull::pullers::SQLPuller;

const DATA_TYPE_QUERY:&str = "
        WITH all_type_defs AS (
            WITH all_types AS (
                SELECT 
                    ns.nspname AS type_schema,
                    typname AS type_name,
                    typtype AS type_type
                FROM pg_type
                JOIN pg_catalog.pg_namespace ns ON typnamespace = ns.oid
                WHERE typname IN (SELECT UNNEST($2))
                AND ns.nspname = $1
            ),
            type_info AS (
                SELECT 
                    type_schema,
                    a.attrelid::regclass AS type_name,
                    E'\t' || attname || ' ' || FORMAT_TYPE(a.atttypid, a.atttypmod) AS attr_def
                FROM pg_type pt
                JOIN pg_catalog.pg_attribute a ON pt.typrelid = a.attrelid OR a.attrelid = pt.typarray 
                JOIN pg_type pt2 ON a.atttypid = pt2.oid
                JOIN pg_catalog.pg_namespace ns ON pt2.typnamespace = ns.oid
                JOIN all_types ON all_types.type_name = pt.typname
                WHERE attnum > 0 AND all_types.type_type = 'c'
                ORDER BY type_name, attnum
            ),
            custom_type_defs AS (
                SELECT 
                    CASE 
                        WHEN type_name::TEXT ILIKE '%.%' THEN 
                            SPLIT_PART(type_name::TEXT, '.', 2) 
                        ELSE type_name::TEXT END 
                    AS name, 
                    'CREATE TYPE ' || type_name || E' AS (\n' ||ARRAY_TO_STRING(ARRAY_AGG(attr_def), E',\n') || E'\n);' AS definition,
                    CASE
                        WHEN type_name::TEXT ILIKE '%.%' THEN 
                            'data_types/' || SPLIT_PART(type_name::TEXT, '.', 2)
                        ELSE 'data_types/' || type_name::TEXT END 
                    AS file_path
                FROM type_info
                GROUP BY type_name, type_schema
            ),
            domain_defs AS (
                SELECT
                    pg_type.typname AS name,
                    'CREATE DOMAIN '
                    || QUOTE_IDENT(schemas.nspname)
                    || '.'
                    || QUOTE_IDENT(pg_type.typname)
                    || ' AS '
                    || FORMAT_TYPE(pg_type.typbasetype, pg_type.typtypmod) 
                    || CASE WHEN pg_type.typnotnull THEN ' NOT NULL' ELSE '' END || E'\n'
                    || COALESCE(' COLLATE ' || QUOTE_IDENT(pg_collation.collname) || E'\n', '')
                    || COALESCE(' DEFAULT ' || pg_type.typdefault || E'\n', '')
                    || COALESCE(string_agg('CONSTRAINT ' || pg_constraint.conname || ' ' || pg_get_constraintdef(pg_constraint.oid, true), '' ORDER BY pg_constraint.oid), '')
                    || ';' AS definition,
                    'data_types/' || pg_type.typname AS file_path
                FROM pg_type
                LEFT JOIN pg_namespace AS schemas ON schemas.oid = pg_type.typnamespace
                LEFT JOIN pg_type AS base_type ON base_type.oid = pg_type.typbasetype
                LEFT JOIN pg_collation ON pg_collation.oid = pg_type.typcollation AND pg_type.typcollation <> base_type.typcollation
                LEFT JOIN pg_constraint ON pg_constraint.contypid = pg_type.oid
                WHERE pg_type.typtype = 'd'
                AND schemas.nspname <> 'information_schema' AND schemas.nspname NOT LIKE 'pg_%'
                AND schemas.nspname = $1 AND pg_type.typname IN (SELECT type_name FROM all_types WHERE type_type = 'd')
                GROUP BY schemas.nspname,
                    pg_type.typname,
                    pg_type.typbasetype,
                    pg_type.typtypmod,
                    pg_collation.collname,
                    pg_type.typnotnull,
                    pg_type.typdefault,
                    pg_type.oid
                ORDER BY schemas.nspname, pg_type.typname
            ),
            enum_defs AS (
                SELECT
                    pg_type.typname AS name,
                    'CREATE TYPE '
                    || QUOTE_IDENT(schemas.nspname)
                    || '.'
                    || QUOTE_IDENT(pg_type.typname)
                    || ' AS ENUM ('
                    ||  STRING_AGG(QUOTE_LITERAL(pg_enum.enumlabel::TEXT), ', ' ORDER BY pg_enum.enumsortorder)
                    || ');' AS definition,
                    'data_types/' || pg_type.typname AS file_path
                FROM pg_enum
                JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
                JOIN pg_namespace AS schemas ON schemas.oid = pg_type.typnamespace
                WHERE schemas.nspname <> 'information_schema' AND schemas.nspname NOT LIKE 'pg_%'
                AND schemas.nspname = $1 AND pg_type.typname IN (SELECT type_name FROM all_types WHERE type_type = 'e')
                GROUP BY schemas.nspname, pg_type.typname, pg_type.oid
                ORDER BY schemas.nspname, pg_type.typname
            )
            SELECT * FROM custom_type_defs
            UNION
            SELECT * FROM domain_defs
            UNION 
            SELECT * FROM enum_defs
        )
        SELECT * FROM all_type_defs
        UNION
        SELECT '', '', 'data_types/' || types.type_name
        FROM (
            SELECT * FROM UNNEST($2) type_name
        ) types
        WHERE types.type_name NOT IN (
            SELECT 
                name
            FROM all_type_defs
        )
            ";

pub struct DataTypePuller {}

impl SQLPuller for DataTypePuller {
    fn get_ddl_query() -> &'static str {
        return DATA_TYPE_QUERY;
    }
}
