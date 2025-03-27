/*-
 * ========================LICENSE_START=================================
 * flyway-xugu
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
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
package org.flywaydb.database.xugu;

import lombok.CustomLog;
import lombok.Getter;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;

@CustomLog
public class XuGuConnection extends Connection<XuGuDatabase> {

    @Getter
    private final boolean awsRds;

    XuGuConnection(XuGuDatabase database, java.sql.Connection connection) {
        super(database, connection);
        awsRds = rdsAdminExists();
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("show current_SCHEMA");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        jdbcTemplate.execute("ALTER SESSION SET CURRENT_SCHEMA=" + database.quote(schema.toUpperCase()));
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

    private boolean rdsAdminExists() {
        try {
            return StringUtils.hasText(jdbcTemplate.queryForString("SELECT USER_NAME FROM ALL_USERS WHERE UPPER(USER_NAME) = 'SYSDBA'"));
        } catch (Exception e) {
            return false;
        }
    }
}
