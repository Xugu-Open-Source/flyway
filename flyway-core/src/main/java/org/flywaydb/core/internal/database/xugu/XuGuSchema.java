/*
 * Copyright © Red Gate Software Ltd 2010-2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.xugu;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * MySQL implementation of Schema.
 */
public class XuGuSchema extends Schema<XuGuDatabase, XuGuTable> {
    /**
     * Creates a new MySQL schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    XuGuSchema(JdbcTemplate jdbcTemplate, XuGuDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT (SELECT 1 FROM information_schema.schemata WHERE schema_name=? LIMIT 1)", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !jdbcTemplate.queryForBoolean("SELECT EXISTS (\n" +
                "    SELECT c.oid FROM pg_catalog.pg_class c\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid AND d.deptype = 'e'\n" +
                "    WHERE  n.nspname = ? AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')\n" +
                "  UNION ALL\n" +
                "    SELECT t.oid FROM pg_catalog.pg_type t\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = t.oid AND d.deptype = 'e'\n" +
                "    WHERE n.nspname = ? AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')\n" +
                "  UNION ALL\n" +
                "    SELECT p.oid FROM pg_catalog.pg_proc p\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'\n" +
                "    WHERE n.nspname = ? AND d.objid IS NULL\n" +
                ")", name, name, name);
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {

        for (String statement : cleanViews()) {
            jdbcTemplate.execute(statement);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        // MariaDB 10.3 and newer only
        for (String statement : cleanSequences()) {
            jdbcTemplate.execute(statement);
        }
    }


    /**
     * Generate the statements to clean the views in this schema.
     *
     * @return The list of statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> cleanViews() throws SQLException {
        List<String> viewNames =
                jdbcTemplate.queryForStringList(
                        "SELECT view_name FROM ALL_views WHERE schema_id=(SELECT schema_id FROM all_schemas WHERE schema_name=?);", name);

        List<String> statements = new ArrayList<>();
        for (String viewName : viewNames) {
            statements.add("DROP VIEW " + database.quote(name, viewName));
        }
        return statements;
    }

    /**
     * Generate the statements to clean the sequences in this schema.
     *
     * @return The list of statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> cleanSequences() throws SQLException {
        List<String> names =
                jdbcTemplate.queryForStringList(
                        "SELECT seq_name FROM ALL_sequences WHERE schema_id=(SELECT schema_id FROM all_schemas WHERE schema_name=?);", name);

        List<String> statements = new ArrayList<>();
        for (String name : names) {
            statements.add("DROP SEQUENCE " + database.quote(this.name, name));
        }
        return statements;
    }

    @Override
    protected XuGuTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList("SELECT table_name FROM ALL_tables WHERE schema_id=(" +
                        "SELECT schema_id FROM all_schemas WHERE schema_name=?);"
                , name);

        XuGuTable[] tables = new XuGuTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new XuGuTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new XuGuTable(jdbcTemplate, database, this, tableName);
    }
}