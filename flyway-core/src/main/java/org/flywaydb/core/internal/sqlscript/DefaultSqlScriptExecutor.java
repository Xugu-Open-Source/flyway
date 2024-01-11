package org.flywaydb.core.internal.sqlscript;

import lombok.CustomLog;
import org.flywaydb.core.api.callback.Error;
import org.flywaydb.core.api.callback.Event;
import org.flywaydb.core.api.callback.Warning;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.AsciiTable;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@CustomLog
public class DefaultSqlScriptExecutor implements SqlScriptExecutor {
    protected final JdbcTemplate jdbcTemplate;

































    public DefaultSqlScriptExecutor(JdbcTemplate jdbcTemplate,
                                    CallbackExecutor callbackExecutor, boolean undo, boolean batch, boolean outputQueryResults,
                                    StatementInterceptor statementInterceptor
                                   ) {
        this.jdbcTemplate = jdbcTemplate;







    }

    @Override
    public void execute(SqlScript sqlScript, Configuration config) {








        try (SqlStatementIterator sqlStatementIterator = sqlScript.getSqlStatements()) {
            SqlStatement sqlStatement;
            while ((sqlStatement = sqlStatementIterator.next()) != null) {





























                    executeStatement(jdbcTemplate, sqlScript, sqlStatement, config);



            }
        }







    }

    protected void logStatementExecution(SqlStatement sqlStatement) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing "



                              + "SQL: " + sqlStatement.getSql());
        }
    }




















































    protected void executeStatement(JdbcTemplate jdbcTemplate, SqlScript sqlScript, SqlStatement sqlStatement, Configuration config) {
        logStatementExecution(sqlStatement);
        String sql = sqlStatement.getSql() + sqlStatement.getDelimiter();






        Results results = sqlStatement.execute(jdbcTemplate



                                              );
        if (results.getException() != null) {





            printWarnings(results);
            handleException(results, sqlScript, sqlStatement, config);
            return;
        }






        printWarnings(results);
        handleResults(results);
    }

    protected void handleResults(Results results) {
        for (Result result : results.getResults()) {
            long updateCount = result.getUpdateCount();
            if (updateCount != -1) {
                handleUpdateCount(updateCount);
            }

            outputQueryResult(result);

        }
    }

    protected void outputQueryResult(Result result) {
        if (



                        result.getColumns() != null && !result.getColumns().isEmpty()) {
            LOG.info(new AsciiTable(result.getColumns(), result.getData(),
                                    true, "", "No rows returned").render());
        }
    }

    private void handleUpdateCount(long updateCount) {
        LOG.debug(updateCount + "row" + StringUtils.pluralizeSuffix(updateCount) + " affected");
    }

    protected void handleException(Results results, SqlScript sqlScript, SqlStatement sqlStatement, Configuration config) {




                throw new FlywaySqlScriptException(sqlScript.getResource(), sqlStatement, results.getException());




    }

    private void printWarnings(Results results) {
        for (Warning warning : results.getWarnings()) {



                if ("00000".equals(warning.getState())) {
                    LOG.info("DB: " + warning.getMessage());
                } else {
                    LOG.warn("DB: " + warning.getMessage()
                                     + " (SQL State: " + warning.getState() + " - Error Code: " + warning.getCode() + ")");
                }



        }
    }
}