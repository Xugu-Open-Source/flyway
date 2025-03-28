/*-
 * ========================LICENSE_START=================================
 * flyway-core
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
package org.flywaydb.core.internal.util;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.util.booleanexpression.ASTBuilderVisitor;
import org.flywaydb.core.internal.util.booleanexpression.ASTEvaluator;
import org.flywaydb.core.internal.util.booleanexpression.ErrorListener;
import org.flywaydb.core.internal.util.booleanexpression.generated.BooleanExpressionLexer;
import org.flywaydb.core.internal.util.booleanexpression.generated.BooleanExpressionParser;

public class BooleanEvaluator {
    /**
     * Evaluates a boolean expression.
     * <p>
     * Currently only supports expressions that are 'true', 'false', 'A==B', 'A!=B' and combinations of those using ( )
     * (precedence), && (AND), || (OR)
     *
     * @param expression The string containing the boolean expression.
     * @return The boolean value the expression evaluates to.
     */
    public static boolean evaluateExpression(final String expression) {
        final var lexer = new BooleanExpressionLexer(CharStreams.fromString(expression));
        final var parser = new BooleanExpressionParser(new CommonTokenStream(lexer));
        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        final var errorListener = new ErrorListener(expression);
        lexer.addErrorListener(errorListener);
        parser.addErrorListener(errorListener);

        final var program = parser.program();
        if (errorListener.hasErrors()) {
            throw new FlywayException("Error parsing boolean expression: " + errorListener.getErrorMessage());
        }

        final var ast = new ASTBuilderVisitor().visitProgram(program);
        return ASTEvaluator.evaluate(ast);
    }
}
