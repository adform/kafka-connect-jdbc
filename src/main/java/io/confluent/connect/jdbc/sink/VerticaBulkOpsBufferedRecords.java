package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.VerticaTempTableDatabaseDialect;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VerticaBulkOpsBufferedRecords {

    private static final Logger log = LoggerFactory.getLogger(VerticaBulkOpsBufferedRecords.class);

    private final TableId tableId;
    private final TableId tmpTableId;
    private final JdbcSinkConfig config;
    private final VerticaTempTableDatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private Map<Object, SinkRecord> deleteOps = new HashMap<>();
    private Map<Object, SinkRecord> insertOps = new HashMap<>();

    private Schema knownKeySchema;
    private Schema knownValueSchema;
    private VerticaBulkOpsBufferedRecords.RecordSchemaDerivedState schemaState;

    public VerticaBulkOpsBufferedRecords(
            JdbcSinkConfig config,
            TableId tableId,
            VerticaTempTableDatabaseDialect dbDialect,
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
        this.schemaState = new VerticaBulkOpsBufferedRecords.RecordSchemaDerivedState(keySchema, valueSchema);
    }

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
            schemaState = new VerticaBulkOpsBufferedRecords.RecordSchemaDerivedState(recordKeySchema, recordValueSchema);
        }

        if (record.value() == null) {
            insertOps.remove(record.key());
            deleteOps.put(record.key(), record);
        } else {
            deleteOps.remove(record.key());
            insertOps.put(record.key(), record);
        }

        if ((insertOps.size() + deleteOps.size()) >= config.batchSize) {
            log.info("Flushing buffered buffer after exceeding configured batch size {}.",
                    config.batchSize);
            flushed.addAll(flush());
        }
        return flushed;
    }

    public void close() throws SQLException {
        schemaState.close();
    }

    public List<SinkRecord> flush() throws SQLException {
        if (insertOps.isEmpty() && deleteOps.isEmpty()) {
            return Collections.emptyList();
        }

        log.debug("TO DELETE: {}, TO INSERT: {}", deleteOps.size(), insertOps.size());
        for (Map.Entry<Object, SinkRecord> e : deleteOps.entrySet()) {
            schemaState.bulkTmpInsertBinder.bindRecord(e.getValue());
        }
        for (Map.Entry<Object, SinkRecord> e : insertOps.entrySet()) {
            SinkRecord toErase = e.getValue();
            SinkRecord sinkRecord = toErase.newRecord(
                    toErase.topic(),
                    toErase.kafkaPartition(),
                    toErase.keySchema(),
                    toErase.key(),
                    toErase.valueSchema(),
                    null,
                    toErase.timestamp());
            schemaState.bulkTmpInsertBinder.bindRecord(sinkRecord);
        }
        for (Map.Entry<Object, SinkRecord> e : insertOps.entrySet()) {
            schemaState.bulkInsertBinder.bindRecord(e.getValue());
        }
        //1. Insert into tem
        int totalACount = 0;
        for (int updateCount : schemaState.bulkTmpInsertStatement.executeBatch()) {
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                totalACount += updateCount;
            }
        }
        log.debug("INSERTED INTO TMP: {}", totalACount);

        //2. Bulk delete from temp
        int deletedRecords;

        try {
            deletedRecords = schemaState.bulkDeleteStatement.executeUpdate();
        } catch (SQLException e) {
            log.error(e.getSQLState());
            throw e;
        }
        boolean deleteSuccess = deletedRecords == (deleteOps.size() + insertOps.size());
        log.debug("DELETE SUCCESS: {}, DELETED RECORDS: {}", deleteSuccess, deletedRecords);

        //3. Bulk insert
        int totalInsertCount = 0;
        if (!insertOps.isEmpty()) {
            for (int updateCount : schemaState.bulkInsertStatement.executeBatch()) {
                if (updateCount != Statement.SUCCESS_NO_INFO) {
                    totalInsertCount += updateCount;
                }
            }
        }
        log.debug("INSERTED INTO DESTINATION: {}", totalInsertCount);

        boolean truncatedTempSuccess;
        try {
            truncatedTempSuccess = schemaState.truncateStatement.execute();
        } catch (SQLException e) {
            log.error("TRUNCATED ERROR {}", e.getSQLState());
            throw e;
        }
        log.debug("TRUNCATE SUCCESS: {}", truncatedTempSuccess);

        checkAffectedRowCount(deletedRecords, totalInsertCount, truncatedTempSuccess);

        connection.commit();

        final List<SinkRecord> flushedRecords = new LinkedList<>();
        flushedRecords.addAll(new ArrayList<>(deleteOps.values()));
        flushedRecords.addAll(new ArrayList<>(insertOps.values()));
        return flushedRecords;
    }

    private void checkAffectedRowCount(int deletedRecords, int insertedRecords, boolean truncate) {
        if(deletedRecords > deleteOps.size() + insertOps.size()){
            throw new ConnectException(String.format(
                    "Deleted row count (%d) did not sum up to total number of buffer inserted/deleted (%d)",
                    deletedRecords,
                    deleteOps.size() + insertOps.size()
            ));
        } else if (insertedRecords != insertOps.size()) {
            throw new ConnectException(String.format(
                    "Inserted count (%d) did not sum up to total number of buffer inserted/deleted (%d)",
                    insertedRecords,
                    insertOps.size()
            ));
        } else if (truncate) {
            throw new ConnectException("Temp table not truncated");
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
    private String getInsertSql(FieldsMetadata fieldsMetadata) {
        return dbDialect.buildInsertStatement(
                tableId,
                asColumns(fieldsMetadata.keyFieldNames),
                asColumns(fieldsMetadata.nonKeyFieldNames)
        );
    }

    /**
     * Inserts go into temporary local table.
     */
    private String getTempInsertSql(FieldsMetadata fieldsMetadata) {
        return dbDialect.buildInsertStatement(
                tmpTableId,
                asColumns(fieldsMetadata.keyFieldNames),
                Collections.emptyList()
        );
    }

    private String getTruncateSql(TableId tableId) {
        return dbDialect.truncateTableStatement(tableId);
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
                    sql = dbDialect.buildBulkDeleteStatement(
                            tableId,
                            tmpTableId,
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

    class RecordSchemaDerivedState {
        private final PreparedStatement bulkDeleteStatement;
        private PreparedStatement bulkInsertStatement;
        private final PreparedStatement truncateStatement;
        private DatabaseDialect.StatementBinder bulkInsertBinder;
        private PreparedStatement bulkTmpInsertStatement;
        private DatabaseDialect.StatementBinder bulkTmpInsertBinder;

        RecordSchemaDerivedState(Schema keySchema, Schema valueSchema) throws SQLException {
            FieldsMetadata fieldsMetadata = checkDatabaseSchema(keySchema, valueSchema);

            log.debug(fieldsMetadata.keyFieldNames.toString());
            log.debug(fieldsMetadata.nonKeyFieldNames.toString());

            final String insertSql = getInsertSql(fieldsMetadata);

            final String deleteSql = getDeleteSql(fieldsMetadata);

            final String truncateSql = getTruncateSql(tmpTableId);

            log.debug(
                    "\n{} sql: {}\nDELETE sql: {}",
                    config.insertMode,
                    insertSql,
                    deleteSql
            );

            // temporary table must exist for insert prepared statement
            // drop & create is performed for case that schema changes in the middle of buffer
            dbDialect.recreateTempTable(
                    connection,
                    tmpTableId.tableName(),
                    fieldsMetadata.allFields.values(),
                    true);

            SchemaPair schemaPair = new SchemaPair(keySchema, valueSchema);

            bulkDeleteStatement = config.deleteEnabled
                    ? connection.prepareStatement(deleteSql) : null;

            truncateStatement = config.deleteEnabled
                    ? connection.prepareStatement(truncateSql) : null;


            if (valueSchema != null) {

                bulkInsertStatement = connection.prepareStatement(insertSql);
                bulkInsertBinder = dbDialect.statementBinder(
                        bulkInsertStatement,
                        config.pkMode,
                        schemaPair,
                        fieldsMetadata,
                        config.insertMode
                );
            }

            bulkTmpInsertStatement = connection.prepareStatement(getTempInsertSql(fieldsMetadata));
            bulkTmpInsertBinder = dbDialect.statementBinder(
                    bulkTmpInsertStatement,
                    config.pkMode,
                    schemaPair,
                    fieldsMetadata,
                    config.insertMode
            );

        }

        void close() throws SQLException {
            if (bulkInsertStatement != null) {
                bulkInsertStatement.close();
            }
            if (bulkTmpInsertStatement != null) {
                bulkTmpInsertStatement.close();
            }
            if (bulkDeleteStatement != null) {
                bulkDeleteStatement.close();
            }
            if (truncateStatement != null) {
                truncateStatement.close();
            }
        }
    }

}
