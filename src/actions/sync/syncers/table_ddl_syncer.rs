use anyhow::Result;
use sqlx::PgPool;

use crate::{
    actions::sync::{
        syncers::{RowStream, Syncer},
        DDL,
    },
    config_file_manager::{format_config_file, get_uncommented_file_contents},
};

pub struct TableDDLSyncer {}

impl Syncer for TableDDLSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>> {
        let file_path = format!(
            "./.dbtvc/config/schemas/{}/table_ddl_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
            WITH tables_to_def AS (
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = $1 AND table_name IN (SELECT * FROM UNNEST($2))
            ),
            table_def AS (
                SELECT 
                    table_schema::TEXT AS schema_name,
                    table_name::TEXT,
                    'CREATE TABLE ' || table_schema || '.' || table_name || E'\n(\n' || ARRAY_TO_STRING(ARRAY_AGG(cols), E',\n') || E'\n);' AS ddl
                FROM (
                    SELECT 
                        table_schema,
                        table_name,
                        E'\t' || column_name || ' ' || data_type ||
                        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END || 
                        CASE WHEN column_default NOT ILIKE 'nextval%' THEN ' DEFAULT ' || column_default ELSE '' END AS cols
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def)
                    ORDER BY ordinal_position
                ) helper
                GROUP BY table_schema, table_name
            ),
            table_owner AS (
                SELECT 
                    schemaname::TEXT AS schema_name,
                    tablename::TEXT AS table_name,
                    'ALTER TABLE ' || schemaname || '.' || tablename || ' OWNER TO ' || tableowner || ';' AS ddl
                FROM pg_tables WHERE schemaname = $1 AND tablename IN (SELECT table_name FROM tables_to_def)
            ),
            sequence_info AS (
                SELECT 
                    n.nspname AS schema_name,
                    t.oid::regclass AS table_name,
                    a.attname AS column_name,
                    s.relname AS sequence_name, 
                    usename AS owner_name
                FROM pg_class AS t
                JOIN pg_attribute AS a ON a.attrelid = t.oid
                JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
                JOIN pg_class AS s ON s.oid = d.objid
                JOIN pg_catalog.pg_namespace n ON n.oid = s.relnamespace
                JOIN pg_user u ON t.relowner = u.usesysid 
                WHERE d.classid = 'pg_catalog.pg_class'::regclass
                AND d.refclassid = 'pg_catalog.pg_class'::regclass
                AND d.deptype IN ('i', 'a')
                AND t.relkind IN ('r', 'P')
                AND s.relkind = 'S'
                AND n.nspname = $1
                AND t.oid::regclass IN (SELECT table_name::regclass FROM tables_to_def)
            ),
            sequence_def_owner AS (
                SELECT 
                    schemaname::TEXT AS schema_name,
                    table_name::TEXT,
                    'CREATE SEQUENCE ' || schemaname || '.' || sequencename || E'\n' || 
                    E'\tAS ' || sq.data_type || E'\n' ||
                    E'\tSTART WITH ' || start_value || E'\n' ||
                    E'\tINCREMENT BY ' || increment_by || E'\n' ||
                    E'\tMINVALUE ' || min_value || E'\n' || 
                    E'\tMAXVALUE ' || max_value || E'\n' ||
                    E'\tCACHE ' || cache_size || E'\n' || 
                    E'\t' || CASE WHEN cycle THEN 'CYCLE;' ELSE 'NO CYCLE;' END || E'\n\n\n' ||
                    'ALTER TABLE ' || schemaname || '.' || sequencename || ' OWNER TO ' || owner_name || ';' || E'\n\n\n' || 
                    'ALTER SEQUENCE ' || schemaname || '.' || sequencename || ' OWNED BY ' || schemaname || '.' || table_name || '.' || column_name || ';' AS ddl
                FROM pg_catalog.pg_sequences sq
                JOIN sequence_info ON sequencename = sequence_name
            ),
            default_col_sequences AS (
                SELECT 
                    table_schema::text AS schema_name,
                    table_name::TEXT,
                    'ALTER TABLE ONLY ' || table_schema || '.' || table_name || ' ALTER COLUMN ' || column_name || ' SET DEFAULT ' || column_default || ';' AS ddl
                FROM (
                    SELECT table_schema,table_name, column_name, column_default 
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def) AND column_default ILIKE 'nextval%'
                    ORDER BY ordinal_position
                ) helper
                GROUP BY table_schema, table_name, column_name, column_default
            ),
            table_constraints AS (
                SELECT
                    ccu.table_schema::TEXT AS schema_name,
                    ccu.table_name::TEXT,
                    'ALTER TABLE ONLY ' || ccu.table_schema || '.' || ccu.table_name || ' ADD CONSTRAINT ' || pgc.conname || ' ' || pg_get_constraintdef(pgc.oid) || ';' AS ddl
                FROM pg_constraint pgc
                JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
                JOIN pg_class cls ON pgc.conrelid = cls.oid
                LEFT JOIN information_schema.constraint_column_usage ccu ON pgc.conname = ccu.constraint_name AND nsp.nspname = ccu.constraint_schema
                WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def)
                ORDER BY pgc.conname
            ),
            index_defs AS (
                SELECT schemaname::TEXT AS schema_name, tablename::TEXT AS table_name, indexdef || ';' AS ddl FROM pg_catalog.pg_indexes
                WHERE schemaname = $1 AND tablename IN (SELECT table_name FROM tables_to_def) AND indexname NOT ILIKE '%pkey%'
            ),
            all_ddl AS (
                SELECT 
                    td.table_name AS name, 
                    ARRAY_TO_STRING(ARRAY[td.ddl,tbo.ddl,sdo.ddl,dcs.ddl,tc.ddl,idef.ddl], E'\n\n\n') AS definition,
                    FORMAT('table_ddl/%I', td.table_name) AS file_path
                FROM table_def td
                LEFT JOIN table_owner tbo ON td.schema_name = tbo.schema_name AND td.table_name = tbo.table_name
                LEFT JOIN sequence_def_owner sdo ON sdo.schema_name = tbo.schema_name AND sdo.table_name = tbo.table_name
                LEFT JOIN default_col_sequences dcs ON dcs.schema_name = tbo.schema_name AND dcs.table_name = tbo.table_name
                LEFT JOIN table_constraints tc ON tc.schema_name = tbo.schema_name AND tc.table_name = tbo.table_name
                LEFT JOIN index_defs idef ON idef.schema_name = tbo.schema_name AND idef.table_name = tbo.table_name
            )
            SELECT * FROM all_ddl
            ")
            .bind(schema)
            .bind(approved_tables)
            .fetch(pool));
    }

    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {
        // TODO: See if this can be changed to have no clones/collects
        let items = items
            .iter()
            .map(|item| {
                let mut new_item = item.clone();
                new_item.push('%');
                return new_item;
            })
            .collect::<Vec<String>>();

        let file_path = format!(
            "./.dbtvc/config/schemas/{}/table_ddl_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
            WITH tables_to_def AS (
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = $1 AND table_name ILIKE ANY(SELECT * FROM UNNEST($2)) AND table_name IN (SELECT * FROM UNNEST($3))
            ),
            table_def AS (
                SELECT 
                    table_schema::TEXT AS schema_name,
                    table_name::TEXT,
                    'CREATE TABLE ' || table_schema || '.' || table_name || E'\n(\n' || ARRAY_TO_STRING(ARRAY_AGG(cols), E',\n') || E'\n);' AS ddl
                FROM (
                    SELECT 
                        table_schema,
                        table_name,
                        E'\t' || column_name || ' ' || data_type ||
                        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END || 
                        CASE WHEN column_default NOT ILIKE 'nextval%' THEN ' DEFAULT ' || column_default ELSE '' END AS cols
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def)
                    ORDER BY ordinal_position
                ) helper
                GROUP BY table_schema, table_name
            ),
            table_owner AS (
                SELECT 
                    schemaname::TEXT AS schema_name,
                    tablename::TEXT AS table_name,
                    'ALTER TABLE ' || schemaname || '.' || tablename || ' OWNER TO ' || tableowner || ';' AS ddl
                FROM pg_tables WHERE schemaname = $1 AND tablename IN (SELECT table_name FROM tables_to_def)
            ),
            sequence_info AS (
                SELECT 
                    n.nspname AS schema_name,
                    t.oid::regclass AS table_name,
                    a.attname AS column_name,
                    s.relname AS sequence_name, 
                    usename AS owner_name
                FROM pg_class AS t
                JOIN pg_attribute AS a ON a.attrelid = t.oid
                JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
                JOIN pg_class AS s ON s.oid = d.objid
                JOIN pg_catalog.pg_namespace n ON n.oid = s.relnamespace
                JOIN pg_user u ON t.relowner = u.usesysid 
                WHERE d.classid = 'pg_catalog.pg_class'::regclass
                AND d.refclassid = 'pg_catalog.pg_class'::regclass
                AND d.deptype IN ('i', 'a')
                AND t.relkind IN ('r', 'P')
                AND s.relkind = 'S'
                AND n.nspname = $1
                AND t.oid::regclass IN (SELECT table_name::regclass FROM tables_to_def)
            ),
            sequence_def_owner AS (
                SELECT 
                    schemaname::TEXT AS schema_name,
                    table_name::TEXT,
                    'CREATE SEQUENCE ' || schemaname || '.' || sequencename || E'\n' || 
                    E'\tAS ' || sq.data_type || E'\n' ||
                    E'\tSTART WITH ' || start_value || E'\n' ||
                    E'\tINCREMENT BY ' || increment_by || E'\n' ||
                    E'\tMINVALUE ' || min_value || E'\n' || 
                    E'\tMAXVALUE ' || max_value || E'\n' ||
                    E'\tCACHE ' || cache_size || E'\n' || 
                    E'\t' || CASE WHEN cycle THEN 'CYCLE;' ELSE 'NO CYCLE;' END || E'\n\n\n' ||
                    'ALTER TABLE ' || schemaname || '.' || sequencename || ' OWNER TO ' || owner_name || ';' || E'\n\n\n' || 
                    'ALTER SEQUENCE ' || schemaname || '.' || sequencename || ' OWNED BY ' || schemaname || '.' || table_name || '.' || column_name || ';' AS ddl
                FROM pg_catalog.pg_sequences sq
                JOIN sequence_info ON sequencename = sequence_name
            ),
            default_col_sequences AS (
                SELECT 
                    table_schema::text AS schema_name,
                    table_name::TEXT,
                    'ALTER TABLE ONLY ' || table_schema || '.' || table_name || ' ALTER COLUMN ' || column_name || ' SET DEFAULT ' || column_default || ';' AS ddl
                FROM (
                    SELECT table_schema,table_name, column_name, column_default 
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def) AND column_default ILIKE 'nextval%'
                    ORDER BY ordinal_position
                ) helper
                GROUP BY table_schema, table_name, column_name, column_default
            ),
            table_constraints AS (
                SELECT
                    ccu.table_schema::TEXT AS schema_name,
                    ccu.table_name::TEXT,
                    'ALTER TABLE ONLY ' || ccu.table_schema || '.' || ccu.table_name || ' ADD CONSTRAINT ' || pgc.conname || ' ' || pg_get_constraintdef(pgc.oid) || ';' AS ddl
                FROM pg_constraint pgc
                JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
                JOIN pg_class cls ON pgc.conrelid = cls.oid
                LEFT JOIN information_schema.constraint_column_usage ccu ON pgc.conname = ccu.constraint_name AND nsp.nspname = ccu.constraint_schema
                WHERE table_schema = $1 AND table_name IN (SELECT table_name FROM tables_to_def)
                ORDER BY pgc.conname
            ),
            index_defs AS (
                SELECT schemaname::TEXT AS schema_name, tablename::TEXT AS table_name, indexdef || ';' AS ddl FROM pg_catalog.pg_indexes
                WHERE schemaname = $1 AND tablename IN (SELECT table_name FROM tables_to_def) AND indexname NOT ILIKE '%pkey%'
            ),
            all_ddl AS (
                SELECT 
                    td.table_name AS name, 
                    ARRAY_TO_STRING(ARRAY[td.ddl,tbo.ddl,sdo.ddl,dcs.ddl,tc.ddl,idef.ddl], E'\n\n\n') AS definition,
                    FORMAT('table_ddl/%I', td.table_name) AS file_path
                FROM table_def td
                LEFT JOIN table_owner tbo ON td.schema_name = tbo.schema_name AND td.table_name = tbo.table_name
                LEFT JOIN sequence_def_owner sdo ON sdo.schema_name = tbo.schema_name AND sdo.table_name = tbo.table_name
                LEFT JOIN default_col_sequences dcs ON dcs.schema_name = tbo.schema_name AND dcs.table_name = tbo.table_name
                LEFT JOIN table_constraints tc ON tc.schema_name = tbo.schema_name AND tc.table_name = tbo.table_name
                LEFT JOIN index_defs idef ON idef.schema_name = tbo.schema_name AND idef.table_name = tbo.table_name
            )
            SELECT * FROM all_ddl
        ")
            .bind(schema)
            .bind(items)
            .bind(approved_tables)
            .fetch(pool));
    }
}
