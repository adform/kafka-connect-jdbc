/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.VerticaMergeDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Local temporary table rows are truncated on commit and no additional
 * permissions are needed for create/drop
 * 1. CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS local_temp_[target_table]
 * 2. INSERT INTO local_temp_[target_table]
 * 3. MERGE INTO [target_table] AS target USING local_temp_[target_table] AS incoming
 * 4. COMMIT (JdbcDbWriter)
 */
public class VerticaBufferedRecords {

  private static final Logger log = LoggerFactory.getLogger(VerticaBufferedRecords.class);

  private final TableId tableId;
  private final TableId tmpTableId;
  private final JdbcSinkConfig config;
  private final VerticaMergeDatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> buffer = new ArrayList<>();

  private Schema knownKeySchema;
  private Schema knownValueSchema;
  private RecordSchemaDerivedState schemaState;
  // flag that we have delete record in buffer
  private boolean deleteInBuffer = false;

  public VerticaBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      VerticaMergeDatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection,
      Schema keySchema,
      Schema valueSchema
  ) throws SQLException {
    this.tableId = tableId;
    String tempTableName = ExpressionBuilder.create().append(tableId, false)
        .toString().replace('.', '_');
    this.tmpTableId = new TableId(null, null, "local_temp_" + tempTableName);
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.knownKeySchema = keySchema;
    this.knownValueSchema = valueSchema;
    this.schemaState = new RecordSchemaDerivedState(keySchema, valueSchema);
  }

  /**
   * Be aware that...
   * Any record can have null schema for key or value. It depends on 'pk.mode' settings.
   * Because use 'delete.enabled=true' which mandates 'pk.mode=record_key',
   * key schema and value in this case are always not null.
   * .
   * Even 1st record of batch can be deletion (record.knownValueSchema() is null)
   * so we will not be able to extract value schema from it
   * In fact all requests in batch can be deletes
   * .
   * From performance point we expect
   * - delete to be rare comparing to inserts and updates
   * - schema changes in batch are extremely rare
   */
  public List<SinkRecord> add(SinkRecord record) throws SQLException {

    final Schema recordKeySchema = record.keySchema();
    final Schema recordValueSchema = record.valueSchema();

    boolean isSchemaChange = (recordKeySchema != null && !recordKeySchema.equals(knownKeySchema))
        || (recordValueSchema != null && !recordValueSchema.equals(knownValueSchema));

    ArrayList<SinkRecord> flushed = new ArrayList<>();
    if (isSchemaChange) {
      // flush buffer with existing schema
      flushed.addAll(flush());
      // reset/reinitialize schema state
      knownKeySchema = recordKeySchema;
      knownValueSchema = recordValueSchema;
      schemaState.close();
      schemaState = new RecordSchemaDerivedState(recordKeySchema, recordValueSchema);
    } else if (record.value() == null) { // isSchemaChange == false
      // delete record - set a flag but do not flush just yet
      deleteInBuffer = true;
    } else if (deleteInBuffer) { // record.value() != null
      // update record following delete record(s) -> flush buffer to preserve order
      flushed.addAll(flush());
    }

    buffer.add(record);
    if (buffer.size() >= config.batchSize) {
      log.debug("Flushing buffered buffer after exceeding configured batch size {}.",
          config.batchSize);
      flushed.addAll(flush());
    }
    return flushed;
  }

  public void close() throws SQLException {
    schemaState.close();
  }

  enum Operation {
    UPSERT, DELETE
  }

  public List<SinkRecord> flush() throws SQLException {
    if (buffer.isEmpty()) {
      return Collections.emptyList();
    }

    // Local table must not contain duplicate keys for MERGE operation -> Keep only last record
    Map<Object, Operation> operations = new HashMap<>();
    for (int i = buffer.size() - 1; i >= 0; --i) { // iterate from latest to oldest
      SinkRecord record = buffer.get(i);
      Object recordKey = record.key();
      Operation operation = operations.get(recordKey);
      if (operations.get(recordKey) == null) {
        if (record.value() != null) {
          schemaState.insertBinder.bindRecord(record);
          operations.put(recordKey, Operation.UPSERT);
        } else {
          schemaState.deleteBinder.bindRecord(record);
          operations.put(recordKey, Operation.DELETE);
        }
      } else {
        log.debug("Skipping record as later {} exist for this key {}", operation, recordKey);
      }
    }

    // 1. Insert into temporary table
    int totalUpdateCount = 0;
    boolean successNoInfo = false;
    for (int updateCount : schemaState.insertStatement.executeBatch()) {
      if (updateCount == Statement.SUCCESS_NO_INFO) {
        successNoInfo = true;
        continue;
      }
      totalUpdateCount += updateCount;
    }

    // 2. Merge temporary table into target table
    int mergeCount = schemaState.mergeStatement.executeUpdate();
    log.debug("Merged {} records from {} to {}", mergeCount, tmpTableId, tableId);

    // 3. Delete(s) from target table
    int totalDeleteCount = 0;
    if (deleteInBuffer) {
      for (int updateCount : schemaState.deleteStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
      log.debug("Deleted {} records from {}", mergeCount, tableId);
    }

    checkAffectedRowCount(totalUpdateCount + totalDeleteCount, successNoInfo);
    // We do commit after every flush to clear temporary local merging table
    // otherwise next round might insert records with same keys into it
    // which makes merge explode on duplicate keys
    connection.commit();

    final List<SinkRecord> flushedRecords = buffer;
    buffer = new ArrayList<>();
    deleteInBuffer = false;
    return flushedRecords;
  }

  private void checkAffectedRowCount(int totalCount, boolean successNoInfo) {
    if (totalCount != buffer.size() && !successNoInfo) {
      switch (config.insertMode) {
        case INSERT:
          throw new ConnectException(String.format(
              "Row count (%d) did not sum up to total number of buffer inserted/deleted (%d)",
              totalCount,
              buffer.size()
          ));
        case UPSERT:
        case UPDATE:
          log.debug(
              "{}/deleted buffer:{} resulting in in totalUpdateCount:{}",
              config.insertMode,
              buffer.size(),
              totalCount
          );
          break;
        default:
          throw new ConnectException("Unknown insert mode: " + config.insertMode);
      }
    }
    if (successNoInfo) {
      log.info(
          "{} buffer:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          buffer.size()
      );
    }
  }

  class RecordSchemaDerivedState {
    private final PreparedStatement deleteStatement;
    private final DatabaseDialect.StatementBinder deleteBinder;
    private final PreparedStatement insertStatement;
    private final DatabaseDialect.StatementBinder insertBinder;
    private final PreparedStatement mergeStatement;

    RecordSchemaDerivedState(Schema keySchema, Schema valueSchema) throws SQLException {
      FieldsMetadata fieldsMetadata = checkDatabaseSchema(keySchema, valueSchema);

      final String insertSql = getTmpInsertSql(fieldsMetadata);
      final String mergeSql = dbDialect.buildMergeTableStatement(
          tmpTableId,
          tableId,
          asColumns(fieldsMetadata.keyFieldNames),
          asColumns(fieldsMetadata.nonKeyFieldNames));
      final String deleteSql = getDeleteSql(fieldsMetadata);

      log.debug(
          "\n{} sql: {}\nDELETE sql: {}\nMERGE sql: {}",
          config.insertMode,
          insertSql,
          deleteSql,
          mergeSql
      );

      // temporary table must exist for insert prepared statement
      // drop & create is performed for case that schema changes in the middle of buffer
      dbDialect.recreateTempTable(
          connection,
          tmpTableId.tableName(),
          fieldsMetadata.allFields.values(),
          false);

      mergeStatement = connection.prepareStatement(mergeSql);

      SchemaPair schemaPair = new SchemaPair(keySchema, valueSchema);

      deleteStatement = config.deleteEnabled
          ? connection.prepareStatement(deleteSql) : null;
      deleteBinder = dbDialect.statementBinder(
          deleteStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata,
          config.insertMode
      );

      insertStatement = connection.prepareStatement(insertSql);
      insertBinder = dbDialect.statementBinder(
          insertStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata,
          config.insertMode
      );

    }

    void close() throws SQLException {
      if (insertStatement != null) {
        insertStatement.close();
      }
      if (mergeStatement != null) {
        mergeStatement.close();
      }
      if (deleteStatement != null) {
        deleteStatement.close();
      }
    }
  }

  private FieldsMetadata checkDatabaseSchema(
      Schema keySchema,
      Schema valueSchema) throws SQLException {
    FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
        tableId.tableName(),
        config.pkMode,
        config.pkFields,
        config.fieldsWhitelist,
        keySchema,
        valueSchema
    );
    dbStructure.createOrAmendIfNecessary(
        config,
        connection,
        tableId,
        fieldsMetadata
    );
    return fieldsMetadata;
  }

  /**
   * Inserts go into temporary local table.
   */
  private String getTmpInsertSql(FieldsMetadata fieldsMetadata) {
    return dbDialect.buildInsertStatement(
        tmpTableId,
        asColumns(fieldsMetadata.keyFieldNames),
        asColumns(fieldsMetadata.nonKeyFieldNames)
    );
  }

  /**
   * Deletes are slow and will go into directly into target table
   * but we have to do it to preserve order of operations.
   */
  private String getDeleteSql(FieldsMetadata fieldsMetadata) {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case NONE:
        case KAFKA:
        case RECORD_VALUE:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          sql = dbDialect.buildDeleteStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames)
          );
          break;
        default:
          break;
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }

}
