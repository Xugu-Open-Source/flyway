/*-
 * ========================LICENSE_START=================================
 * flyway-verb-utils
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
package org.flywaydb.verb;

import static org.flywaydb.core.internal.sqlscript.FlywaySqlScriptException.STATEMENT_MESSAGE;

import java.sql.SQLException;
import lombok.CustomLog;
import org.flywaydb.core.api.resource.LoadableResource;
import org.flywaydb.core.internal.sqlscript.SqlStatement;
import org.flywaydb.core.internal.util.ExceptionUtils;
import org.flywaydb.core.internal.util.StringUtils;

@CustomLog
public class ErrorUtils {

    public static String calculateErrorMessage(final Exception e,
        final String title,
        final LoadableResource loadableResource,
        final String physicalLocation,
        final SqlStatement sqlStatement,
        final String message) {

        final String underline = StringUtils.trimOrPad("", title.length(), '-');

        final StringBuilder messageBuilder = new StringBuilder().append(title).append("\n").append(underline).append(
            "\n");

        if (message != null) {
            messageBuilder.append(message);
        }

        if (loadableResource != null) {
            messageBuilder.append("Location   : ")
                .append(loadableResource.getAbsolutePath())
                .append(" (")
                .append(loadableResource.getAbsolutePathOnDisk())
                .append(")\n");
        } else {
            messageBuilder.append("Location   : ").append(physicalLocation);
        }

        if (sqlStatement != null) {
            messageBuilder.append("Line       : ").append(sqlStatement.getLineNumber()).append("\n");
            messageBuilder.append("Statement  : ").append(LOG.isDebugEnabled()
                ? sqlStatement.getSql()
                : STATEMENT_MESSAGE).append("\n");
        }
        return messageBuilder.toString();
    }
}
