package org.flywaydb.core.internal.database.xugu;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.DatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.Properties;

public class XuGuDatabaseType extends DatabaseType {
    private static final String XUGU_LEGACY_JDBC_DRIVER = "com.xugu.cloudjdbc.Driver";

    @Override
    public String getName() {
        return "XuGu";
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        if (url.startsWith("jdbc-secretsmanager:xugu:")) {
            throw new org.flywaydb.core.internal.license.FlywayTeamsUpgradeRequiredException("jdbc-secretsmanager");
        }
        return url.startsWith("jdbc:xugu:");
    }

//    @Override
//    public Pattern getJDBCCredentialsPattern() {
//        return usernamePasswordPattern;
//    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "com.xugu.cloudjdbc.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return databaseProductName.contains("XuGu");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new XuGuDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new XuGuParser(configuration, parsingContext);
    }

    @Override
    public SqlScriptExecutorFactory createSqlScriptExecutorFactory(JdbcConnectionFactory jdbcConnectionFactory,
                                                                   final CallbackExecutor callbackExecutor,
                                                                   final StatementInterceptor statementInterceptor
    ) {
        final DatabaseType thisRef = this;
        return new SqlScriptExecutorFactory() {
            @Override
            public SqlScriptExecutor createSqlScriptExecutor(Connection connection , boolean undo, boolean batch, boolean outputQueryResults) {
                return new XuGuSqlScriptExecutor(new JdbcTemplate(connection, thisRef), callbackExecutor, undo, batch, outputQueryResults, statementInterceptor);
            }
        };
    }

    @Override
    public void setDefaultConnectionProps(String url, Properties props, ClassLoader classLoader) {

    }

    @Override
    public void setConfigConnectionProps(Configuration config, Properties props, ClassLoader classLoader) {
    }
}