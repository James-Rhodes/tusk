use anyhow::Result;
use sqlx::PgPool;

use crate::{
    actions::sync::{
        syncers::{RowStream, SQLSyncer},
        DDL,
    },
    config_file_manager::{
        format_config_file, get_matching_uncommented_file_contents, get_uncommented_file_contents,
    },
};

const DATA_TYPE_QUERY:&str = "
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
                SELECT type_schema,
                    a.attrelid::regclass AS type_name,
                    E'\t' || attname || ' ' ||  CASE WHEN ns.nspname = 'pg_catalog' THEN '' ELSE ns.nspname || '.' END || FORMAT_TYPE(a.atttypid, a.atttypmod) AS attr_def
                FROM pg_attribute a
                JOIN pg_catalog.pg_type pt ON pt.oid = a.atttypid 
                JOIN pg_catalog.pg_namespace ns ON typnamespace = ns.oid
                JOIN all_types ON all_types.type_name = a.attrelid::REGCLASS::TEXT
                WHERE attnum > 0 AND all_types.type_type = 'c'
            ),
            custom_type_defs AS (
                SELECT 
                    type_name::TEXT AS name, 
                    'CREATE TYPE ' || type_schema || '.'|| type_name || E' AS (\n' ||ARRAY_TO_STRING(ARRAY_AGG(attr_def), E',\n') || E'\n);' AS definition,
                    'data_types/' || type_name AS file_path
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
            ";

pub struct DataTypeSyncer {}

impl SQLSyncer for DataTypeSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>> {
        let file_path = format!(
            "./.tusk/config/schemas/{}/data_types_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_data_types = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>(DATA_TYPE_QUERY)
            .bind(schema)
            .bind(approved_data_types)
            .fetch(pool));
    }

    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {
        let file_path = format!(
            "./.tusk/config/schemas/{}/data_types_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_data_types = get_uncommented_file_contents(&file_path)?;
        let items =
            get_matching_uncommented_file_contents(&approved_data_types, &items, Some(schema))?
                .into_iter()
                .map(|item| item.clone())
                .collect::<Vec<String>>();

        return Ok(sqlx::query_as::<_, DDL>(DATA_TYPE_QUERY)
            .bind(schema)
            .bind(items)
            .fetch(pool));
    }
}
