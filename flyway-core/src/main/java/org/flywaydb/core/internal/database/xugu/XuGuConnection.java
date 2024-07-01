package org.flywaydb.core.internal.database.xugu;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

import java.sql.SQLException;

public class XuGuConnection extends Connection<XuGuDatabase> {
    XuGuConnection(XuGuDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("show current_SCHEMA");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        jdbcTemplate.execute("ALTER SESSION SET CURRENT_SCHEMA=" + database.quote(schema));
    }

    @Override
    protected Schema doGetCurrentSchema() throws SQLException {
        String schemaName = getCurrentSchemaNameOrSearchPath();
        return schemaName == null ? null : getSchema(schemaName);
    }

    @Override
    public Schema getSchema(String name) {
        return new XuGuSchema(jdbcTemplate, database, name);
    }
}