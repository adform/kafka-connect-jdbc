/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class VerticaMergeDatabaseDialect extends VerticaDatabaseDialect {
  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public VerticaMergeDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  /**
   * @param sourceTable table to merge from (local temporary table)
   * @param targetTable table to merge into
   * @param keyColumns primary key columns to join on
   * @param nonKeyColumns columns to be merged
   * @return MERGE SQL string
   */
  public String buildMergeTableStatement(
      TableId sourceTable,
      TableId targetTable,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ");
    builder.append(targetTable);
    builder.append(" AS target USING ");
    builder.append(sourceTable);
    builder.append(" AS incoming ON (");
    builder.appendList()
        .delimitedBy(" AND ")
        .transformedBy(this::transformAs)
        .of(keyColumns);
    builder.append(")");
    builder.append(" WHEN MATCHED THEN UPDATE SET ");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(this::transformUpdate)
        .of(nonKeyColumns, keyColumns);
    builder.append(" WHEN NOT MATCHED THEN INSERT (");
    builder.appendList()
        .delimitedBy(", ")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix(""))
        .of(nonKeyColumns, keyColumns);
    builder.append(") VALUES (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
        .of(nonKeyColumns, keyColumns);
    builder.append(");");
    return builder.toString();
  }

  /**
   * @param connection the database connection; may not be null
   * @param table name of the table to be created. Must not be qualified
   * @param fields record fields to be sinked
   * @param preserveOnCommit Use 'ON COMMIT PRESERVE ROWS' but default is false
   * @throws SQLException thrown by database
   */
  public void recreateTempTable(
      Connection connection, String table,
      Collection<SinkRecordField> fields,
      boolean preserveOnCommit) throws SQLException {
    String dropSql = "DROP TABLE IF EXISTS " + table;
    String createSql = buildCreateTempTableStatement(table, fields, preserveOnCommit);
    //log.debug("Table SQL {}", createSql);
    applyDdlStatements(connection, Arrays.asList(dropSql, createSql));
  }

  private String buildCreateTempTableStatement(
      String table,
      Collection<SinkRecordField> fields, boolean preserveOnCommit
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    if (!pkFieldNames.isEmpty()) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.quote())
          .of(pkFieldNames);
      builder.append(")");
    }
    builder.append(")");
    if (preserveOnCommit) {
      builder.append(" ON COMMIT PRESERVE ROWS");
    }
    return builder.toString();
  }

  private void transformAs(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
        .appendIdentifierQuoted(col.name())
        .append("=incoming.")
        .appendIdentifierQuoted(col.name());
  }

  private void transformUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.appendIdentifierQuoted(col.name())
        .append("=incoming.")
        .appendIdentifierQuoted(col.name());
  }

}
