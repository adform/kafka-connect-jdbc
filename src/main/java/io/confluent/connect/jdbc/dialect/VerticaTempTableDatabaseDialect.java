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
import java.util.stream.Collectors;

public class VerticaTempTableDatabaseDialect extends VerticaDatabaseDialect {
  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public VerticaTempTableDatabaseDialect(AbstractConfig config) {
    super(config);
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
      writeColumnsSpec(builder, fields.stream().filter(SinkRecordField::isPrimaryKey).collect(Collectors.toList()));
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

}
