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

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.*;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class XuGuParser extends Parser {

    private static final Delimiter PLSQL_DELIMITER = new Delimiter("/", true);
    //                                                 accessible   by    (        keyword<space>optionalidentifier                      )
    private static final String ACCESSIBLE_BY_REGEX = "ACCESSIBLE\\sBY\\s\\(?(((FUNCTION|PROCEDURE|PACKAGE|TRIGGER|TYPE)\\s)?[^\\s]*\\s?+)*\\)?";

    private static final Pattern PLSQL_PACKAGE_BODY_REGEX = Pattern.compile(
            "^CREATE(\\s*OR\\s*REPLACE)?(\\s*(NON)?EDITIONABLE)?\\s*PACKAGE\\s*BODY\\s*([^\\s]*\\s)?(IS|AS)");
    private static final StatementType PLSQL_PACKAGE_BODY_STATEMENT = new StatementType();

    private static final Pattern PLSQL_PACKAGE_DEFINITION_REGEX = Pattern.compile(
            "^CREATE(\\s*OR\\s*REPLACE)?(\\s*(NON)?EDITIONABLE)?\\s*PACKAGE\\s([^\\s*]*\\s*)?(AUTHID\\s*[^\\s*]*\\s*|" + ACCESSIBLE_BY_REGEX + ")*(IS|AS)");

    private static final Pattern PLSQL_VIEW_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sVIEW\\s([^\\s]*\\s)?AS\\sWITH\\s(PROCEDURE|FUNCTION)");
    private static final StatementType PLSQL_VIEW_STATEMENT = new StatementType();

    private static final Pattern PLSQL_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\s(FUNCTION(\\s\\S*)|PROCEDURE|TYPE|TRIGGER)");
    private static final Pattern DECLARE_BEGIN_REGEX = Pattern.compile("^DECLARE|BEGIN|WITH");
    private static final StatementType PLSQL_STATEMENT = new StatementType();

    private static final Pattern JAVA_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\sAND\\s(RESOLVE|COMPILE))?(\\sNOFORCE)?\\sJAVA\\s(SOURCE|RESOURCE|CLASS)");
    private static final StatementType PLSQL_JAVA_STATEMENT = new StatementType();

    private static final Pattern PLSQL_PACKAGE_BODY_WRAPPED_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE\\sBODY(\\s\\S*)?\\sWRAPPED(\\s\\S*)*");
    private static final Pattern PLSQL_PACKAGE_DEFINITION_WRAPPED_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE(\\s\\S*)?\\sWRAPPED(\\s\\S*)*");
    private static final Pattern PLSQL_WRAPPED_REGEX = Pattern.compile(
            "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\s(FUNCTION|PROCEDURE|TYPE)(\\s\\S*)?\\sWRAPPED(\\s\\S*)*");

    private static final StatementType PLSQL_WRAPPED_STATEMENT = new StatementType();
    private int initialWrappedBlockDepth = -1;

    public XuGuParser(Configuration configuration, ParsingContext parsingContext
    ) {
        super(configuration, parsingContext, 3);
    }

    @Override
    protected ParsedSqlStatement createStatement(PeekingReader reader, Recorder recorder,
                                                 int statementPos, int statementLine, int statementCol,
                                                 int nonCommentPartPos, int nonCommentPartLine, int nonCommentPartCol,
                                                 StatementType statementType, boolean canExecuteInTransaction,
                                                 Delimiter delimiter, String sql
            , boolean batchable
    ) throws IOException {
        if (PLSQL_VIEW_STATEMENT == statementType) {
            sql = sql.trim();

            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
        }
        return super.createStatement(reader, recorder, statementPos, statementLine, statementCol,
                nonCommentPartPos, nonCommentPartLine, nonCommentPartCol,
                statementType, canExecuteInTransaction, delimiter, sql
                , batchable
        );
    }

    @Override
    protected StatementType detectStatementType(String simplifiedStatement, ParserContext context, PeekingReader reader) {
        if (PLSQL_PACKAGE_BODY_WRAPPED_REGEX.matcher(simplifiedStatement).matches()
                || PLSQL_PACKAGE_DEFINITION_WRAPPED_REGEX.matcher(simplifiedStatement).matches()
                || PLSQL_WRAPPED_REGEX.matcher(simplifiedStatement).matches()) {
            if (initialWrappedBlockDepth == -1) {
                initialWrappedBlockDepth = context.getBlockDepth();
            }
            return PLSQL_WRAPPED_STATEMENT;
        }

        if (PLSQL_PACKAGE_BODY_REGEX.matcher(simplifiedStatement).matches()) {
            return PLSQL_PACKAGE_BODY_STATEMENT;
        }

        if (PLSQL_REGEX.matcher(simplifiedStatement).matches()
                || PLSQL_PACKAGE_DEFINITION_REGEX.matcher(simplifiedStatement).matches()
                || DECLARE_BEGIN_REGEX.matcher(simplifiedStatement).matches()) {
            try {
                String wrappedKeyword = " WRAPPED";
                if (!reader.peek(wrappedKeyword.length()).equalsIgnoreCase(wrappedKeyword)) {
                    return PLSQL_STATEMENT;
                }
            } catch (IOException e) {
                return PLSQL_STATEMENT;
            }
        }

        if (JAVA_REGEX.matcher(simplifiedStatement).matches()) {
            return PLSQL_JAVA_STATEMENT;
        }

        if (PLSQL_VIEW_REGEX.matcher(simplifiedStatement).matches()) {
            return PLSQL_VIEW_STATEMENT;
        }
        return super.detectStatementType(simplifiedStatement, context, reader);
    }

    @Override
    protected boolean shouldDiscard(Token token, boolean nonCommentPartSeen) {
        // Discard dangling PL/SQL / delimiters
        return ("/".equals(token.getText()) && !nonCommentPartSeen) || super.shouldDiscard(token, nonCommentPartSeen);
    }

    @Override
    protected void adjustDelimiter(ParserContext context, StatementType statementType) {
        if (statementType == PLSQL_STATEMENT || statementType == PLSQL_VIEW_STATEMENT || statementType == PLSQL_JAVA_STATEMENT
                || statementType == PLSQL_PACKAGE_BODY_STATEMENT) {
            context.setDelimiter(PLSQL_DELIMITER);


        } else {
            context.setDelimiter(Delimiter.SEMICOLON);
        }
    }

    @Override
    protected boolean shouldAdjustBlockDepth(ParserContext context, List<Token> tokens, Token token) {
        return token.getParensDepth() == 0;
    }

    private static final StatementType STORED_PROGRAM_STATEMENT = new StatementType();

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) {
        String keywordText = keyword.getText();

        int parensDepth = keyword.getParensDepth();

        if ("BEGIN".equalsIgnoreCase(keywordText) && context.getStatementType() == STORED_PROGRAM_STATEMENT) {
            context.increaseBlockDepth("");
        }
        if ("CASE".equalsIgnoreCase(keywordText)) {

            if (!lastTokenIs(tokens, parensDepth, "END")) {
                context.increaseBlockDepth("");
            }
        }
        if (context.getBlockDepth() > 0
                && lastTokenIs(tokens, parensDepth, "END")
                && !"IF".equalsIgnoreCase(keywordText)
                && !"LOOP".equalsIgnoreCase(keywordText)
                && !"REPEAT".equalsIgnoreCase(keywordText)
                && !"WHILE".equalsIgnoreCase(keywordText)) {
            context.decreaseBlockDepth();
        }
    }



    @Override
    protected boolean isDelimiter(String peek, ParserContext context, int col, int colIgnoringWhitespace) {
        Delimiter delimiter = context.getDelimiter();

        if (peek.startsWith(delimiter.getEscape() + delimiter.getDelimiter())) {
            return true;
        }

        if (delimiter.shouldBeAloneOnLine()) {
            // Only consider alone-on-line delimiters (such as "/" for PL/SQL) if
            // it's the first character on the line
            return colIgnoringWhitespace == 1 && peek.trim().equals(delimiter.getDelimiter());
        } else {
            if (colIgnoringWhitespace == 1 && "/".equals(peek.trim())) {
                return true;
            }
        }

        return super.isDelimiter(peek, context, col, colIgnoringWhitespace);
    }

    @Override
    protected boolean isAlternativeStringLiteral(String peek) {
        if (peek.length() < 3) {
            return false;
        }
        char firstChar = peek.charAt(0);
        return (firstChar == 'q' || firstChar == 'Q') && peek.charAt(1) == '\'';
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        reader.swallow(2);
        String closeQuote = computeAlternativeCloseQuote((char) reader.read());
        reader.swallowUntilExcluding(closeQuote);
        reader.swallow(closeQuote.length());
        return new Token(TokenType.STRING, pos, line, col, null, null, context.getParensDepth());
    }

    private String computeAlternativeCloseQuote(char specialChar) {
        switch (specialChar) {
            case '!':
                return "!'";
            case '[':
                return "]'";
            case '(':
                return ")'";
            case '{':
                return "}'";
            case '<':
                return ">'";
            default:
                return specialChar + "'";
        }
    }
}