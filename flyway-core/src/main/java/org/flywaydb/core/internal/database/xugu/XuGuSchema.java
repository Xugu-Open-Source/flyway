/*
 * Copyright (C) Red Gate Software Ltd 2010-2021
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

public class XuGuSchema extends Schema<XuGuDatabase, XuGuTable> {
    XuGuSchema(JdbcTemplate jdbcTemplate, XuGuDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT * FROM ALL_schemas WHERE schema_name = ?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM (SELECT obj_id FROM all_objects \n" +
                "WHERE obj_id NOT IN \n" +
                "(SELECT so.obj_id FROM all_objects so \n" +
                "JOIN all_depends sd ON so.obj_id=sd.obj_id1 OR so.obj_id=sd.obj_id2 AND so.db_id=sd.db_id) " +
                "AND schema_id=(SELECT schema_id FROM all_schemas  WHERE schema_name=?));", name) == 0;
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
        for (String statement : generateDropTypeStatements()) {
            jdbcTemplate.execute(statement);
        }

        for (String dropProcedureStmt : generateDropProcedureStatements()) {
            jdbcTemplate.execute(dropProcedureStmt);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        for (String statement : cleanSequences()) {
            jdbcTemplate.execute(statement);
        }
    }

    private List<String> generateDropTypeStatements() throws SQLException {
        List<String> procedureNames = jdbcTemplate.queryForStringList(
                "SELECT type_name FROM ALL_TYPES WHERE schema_id = (SELECT schema_id FROM all_schemas WHERE schema_name=?);", name);
        List<String> statements = new ArrayList<>();
        for (String typeName : procedureNames) {
            statements.add("DROP TYPE " + database.quote(name, typeName));
        }
        return statements;
    }

    private List<String> generateDropProcedureStatements() throws SQLException {
        List<String> procedureNames = jdbcTemplate.queryForStringList(
                "SELECT  proc_name FROM ALL_PROCEDURES WHERE schema_id=(SELECT schema_id FROM all_schemas WHERE schema_name=?);", name);
        return generateDropStatements("procedure", procedureNames);
    }

    private List<String> generateDropStatements(String objectType, List<String> objectNames) {
        List<String> statements = new ArrayList<>(objectNames.size());
        for (String objectName : objectNames) {
            statements.add("drop " + objectType + " " + database.quote(objectName));
        }
        return statements;
    }

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