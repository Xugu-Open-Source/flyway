package org.flywaydb.database.sqlserver.synapse;

import org.flywaydb.database.sqlserver.SQLServerDatabase;
import org.flywaydb.database.sqlserver.SQLServerSchema;
import org.flywaydb.database.sqlserver.SQLServerTable;
import org.flywaydb.core.internal.database.InsertRowLock;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

public class SynapseTable extends SQLServerTable {
    private final InsertRowLock insertRowLock;

    SynapseTable(JdbcTemplate jdbcTemplate, SQLServerDatabase database, String databaseName, SQLServerSchema schema, String name) {
        super(jdbcTemplate, database, databaseName, schema, name);
        this.insertRowLock = new InsertRowLock(jdbcTemplate);
    }

    @Override
    protected void doLock() throws SQLException {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Timestamp currentDateTime = new Timestamp(cal.getTime().getTime());

        String updateLockStatement = "UPDATE " + this + " SET installed_on = '" + currentDateTime + "' WHERE version = '?' AND description = 'flyway-lock'";
        String deleteExpiredLockStatement = "DELETE FROM " + this + " WHERE description = 'flyway-lock' AND installed_on < '?'";

        if (lockDepth == 0) {
            insertRowLock.doLock(database.getInsertStatement(this), updateLockStatement, deleteExpiredLockStatement, database.getBooleanTrue());
        }
    }

    @Override
    protected void doUnlock() throws SQLException {
        if (lockDepth == 1) {
            insertRowLock.doUnlock(getDeleteLockTemplate());
        }
    }

    private String getDeleteLockTemplate() {
        return "DELETE FROM " + this + " WHERE version = '?' AND description = 'flyway-lock'";
    }
}