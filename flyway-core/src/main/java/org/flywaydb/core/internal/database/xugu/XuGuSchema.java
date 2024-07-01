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
        return !jdbcTemplate.queryForBoolean("SELECT 1 FROM dual WHERE EXISTS (SELECT obj_id FROM all_objects \n" +
                "WHERE obj_id NOT IN \n" +
                "(SELECT so.obj_id FROM all_objects so \n" +
                "JOIN all_depends sd ON so.obj_id=sd.obj_id1 OR so.obj_id=sd.obj_id2 AND so.db_id=sd.db_id) " +
                "AND schema_id=(SELECT schema_id FROM all_schemas  WHERE schema_name=?));", name);
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

        for (String statement : cleanSequences()) {
            jdbcTemplate.execute(statement);
        }
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