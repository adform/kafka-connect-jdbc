package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.VerticaDatabaseDialect;
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
    private final VerticaDatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private Map<Object, SinkRecord> deleteOps = new HashMap<>();
    private Map<Object, SinkRecord> insertOps = new HashMap<>();

    private Schema knownKeySchema;
    private Schema knownValueSchema;
    private VerticaBulkOpsBufferedRecords.RecordSchemaDerivedState schemaState;
    // flag that we have delete record in buffer
    private boolean deleteInBuffer = false;

    public VerticaBulkOpsBufferedRecords(
            JdbcSinkConfig config,
            TableId tableId,
            VerticaDatabaseDialect dbDialect,
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
            schemaState = new VerticaBulkOpsBufferedRecords.RecordSchemaDerivedState(recordKeySchema, recordValueSchema);
        } else if (record.value() == null) { // isSchemaChange == false
            // delete record - set a flag but do not flush just yet
            deleteInBuffer = true;
        } else if (deleteInBuffer) { // record.value() != null
            // update record following delete record(s) -> flush buffer to preserve order
            flushed.addAll(flush());
        }

        if (record.value() == null) {
            insertOps.remove(record.key());
            deleteOps.put(record.key(), record);
        } else {
            deleteOps.remove(record.key());
            insertOps.put(record.key(), record);
        }

        if ((insertOps.size() + deleteOps.size()) >= config.batchSize) {
            log.debug("Flushing buffered buffer after exceeding configured batch size {}.",
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

        for (Map.Entry<Object, SinkRecord> e : deleteOps.entrySet()) {
            schemaState.bulkDeleteBinder.bindRecord(e.getValue());
        }
        for (Map.Entry<Object, SinkRecord> e : insertOps.entrySet()) {
            schemaState.bulkDeleteBinder.bindRecord(e.getValue());
        }
        for (Map.Entry<Object, SinkRecord> e : insertOps.entrySet()) {
            schemaState.bulkInsertBinder.bindRecord(e.getValue());
        }


        //1. Delete
        int totalDeleteCount = 0;
        for (int updateCount : schemaState.bulkDeleteStatement.executeBatch()) {
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                totalDeleteCount += updateCount;
            }
        }


        //2. Bulk delete all records with sub query
        int totalInsertCount = 0;
        for (int updateCount : schemaState.bulkInsertStatement.executeBatch()) {
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                totalInsertCount += updateCount;
            }
        }

        checkAffectedRowCount(totalInsertCount + totalDeleteCount, true);


        // We do commit after every flush to clear temporary local merging table
        // otherwise next round might insert records with same keys into it
        // which makes merge explode on duplicate keys
        connection.commit();


        final List<SinkRecord> flushedRecords = new LinkedList<>();
        flushedRecords.addAll(new ArrayList<>(deleteOps.values()));
        flushedRecords.addAll(new ArrayList<>(insertOps.values()));
        return flushedRecords;
    }

    private void checkAffectedRowCount(int totalCount, boolean successNoInfo) {
        int total = deleteOps.size() + insertOps.size() + insertOps.size();
        if (totalCount != total && !successNoInfo) {
            switch (config.insertMode) {
                case INSERT:
                    throw new ConnectException(String.format(
                            "Row count (%d) did not sum up to total number of buffer inserted/deleted (%d)",
                            totalCount,
                            total
                    ));
                case UPSERT:
                case UPDATE:
                    log.debug(
                            "{}/deleted buffer:{} resulting in in totalUpdateCount:{}",
                            config.insertMode,
                            total,
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
                    total
            );
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

    enum Operation {
        UPSERT, DELETE
    }

    class RecordSchemaDerivedState {
        private final PreparedStatement bulkDeleteStatement;
        private final DatabaseDialect.StatementBinder bulkDeleteBinder;
        private final PreparedStatement bulkInsertStatement;
        private final DatabaseDialect.StatementBinder bulkInsertBinder;

        RecordSchemaDerivedState(Schema keySchema, Schema valueSchema) throws SQLException {
            FieldsMetadata fieldsMetadata = checkDatabaseSchema(keySchema, valueSchema);

            final String insertSql = getInsertSql(fieldsMetadata);

            final String deleteSql = getDeleteSql(fieldsMetadata);

            log.debug(
                    "\n{} sql: {}\nDELETE sql: {}",
                    config.insertMode,
                    insertSql,
                    deleteSql
            );

            SchemaPair schemaPair = new SchemaPair(keySchema, valueSchema);

            bulkDeleteStatement = config.deleteEnabled
                    ? connection.prepareStatement(deleteSql) : null;
            bulkDeleteBinder = dbDialect.statementBinder(
                    bulkDeleteStatement,
                    config.pkMode,
                    schemaPair,
                    fieldsMetadata,
                    config.insertMode
            );

            bulkInsertStatement = connection.prepareStatement(insertSql);
            bulkInsertBinder = dbDialect.statementBinder(
                    bulkInsertStatement,
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
            if (bulkDeleteStatement != null) {
                bulkDeleteStatement.close();
            }
        }
    }

}
