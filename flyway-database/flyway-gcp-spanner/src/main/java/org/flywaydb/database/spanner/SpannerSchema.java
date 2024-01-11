package org.flywaydb.database.spanner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;






import java.sql.SQLException;
import java.util.List;

@CustomLog
public class SpannerSchema extends Schema<SpannerDatabase, SpannerTable> {

    public SpannerSchema(JdbcTemplate jdbcTemplate, SpannerDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() {
        return name.equals("");
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        try (Connection c = database.getNewRawConnection()) {
            Statement s = c.createStatement();
            s.close();
            try (ResultSet tables = c.getMetaData().getTables("", "", null, null)) {
                return !tables.next();
            }
        }
    }

    @Override
    protected void doCreate() {
        LOG.info("Spanner does not support creating schemas. Schema not created: " + name);
    }

    @Override
    protected void doDrop() throws SQLException {
        doClean();
    }

    @Override
    protected void doClean() throws SQLException {
        List<String> statements = new ArrayList<>();

        for (String[] foreignKeyAndTable : doAllForeignKeys()) {
            String foreignKey = foreignKeyAndTable[0];
            String table = foreignKeyAndTable[1];
            statements.add("ALTER TABLE " + table + " DROP CONSTRAINT " + foreignKey);
        }
        executeStatements(statements);

        for (String view : doAllViews()) {
            statements.add("DROP VIEW " + view);
        }
        executeStatements(statements);

        for (Table table : doAllTables()) {
            for (String index : doAllIndexes(table)) {
                if (!index.equalsIgnoreCase("PRIMARY_KEY")) {
                    jdbcTemplate.execute("DROP INDEX " + index);
                }
            }
            statements.add("DROP TABLE " + table);
        }
        executeStatements(statements);
    }

    private void executeStatements(List<String> statements) throws SQLException {















         for (String statement : statements) {
             jdbcTemplate.execute(statement);
         }


        statements.clear();
    }

    private String[] doAllViews() throws SQLException {
        List<String> viewList = new ArrayList<>();
        Connection connection = jdbcTemplate.getConnection();

        ResultSet viewResults = connection.getMetaData().getTables("", "", null, new String[]{ "VIEW" });
        while (viewResults.next()) {
            viewList.add(viewResults.getString("TABLE_NAME"));
        }
        viewResults.close();

        return  viewList.toArray(new String[0]);
    }

    @Override
    protected SpannerTable[] doAllTables() throws SQLException {
        List<SpannerTable> tablesList = new ArrayList<>();
        Connection connection = jdbcTemplate.getConnection();

        ResultSet tablesRs = connection.getMetaData().getTables("", "", null, new String[]{ "TABLE" });
        while (tablesRs.next()) {
            tablesList.add(new SpannerTable(jdbcTemplate, database, this,
                                            tablesRs.getString("TABLE_NAME")));
        }
        tablesRs.close();

        SpannerTable[] tables = new SpannerTable[tablesList.size()];
        return tablesList.toArray(tables);
    }

    private List<String[]> doAllForeignKeys() {
        List<String[]> foreignKeyAndTableList = new ArrayList<>();

        Results foreignKeyRs = jdbcTemplate.executeStatement("SELECT CONSTRAINT_NAME, TABLE_NAME " +
                                                                     "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
                                                                     "WHERE CONSTRAINT_TYPE='FOREIGN KEY'");

        for (Result result : foreignKeyRs.getResults()) {
            for (List<String> row : result.getData()) {
                String[] foreignKeyAndTable = {row.get(0), row.get(1)};
                foreignKeyAndTableList.add(foreignKeyAndTable);
            }
        }

        return foreignKeyAndTableList;
    }

    private List<String> doAllIndexes(Table table) throws SQLException {
        List<String> indexList = new ArrayList<>();
        Connection c = jdbcTemplate.getConnection();

        ResultSet indexRs = c.getMetaData().getIndexInfo("", "", table.getName(), false, false);
        while (indexRs.next()) {
            indexList.add(indexRs.getString("INDEX_NAME"));
        }
        indexRs.close();

        return indexList;
    }

    @Override
    public Table getTable(String tableName) {
        return new SpannerTable(jdbcTemplate, database, this, tableName);
    }
}