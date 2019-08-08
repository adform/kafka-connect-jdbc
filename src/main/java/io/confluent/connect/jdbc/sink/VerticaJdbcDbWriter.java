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

import io.confluent.connect.jdbc.dialect.VerticaDatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Replace BufferedRecords with custom VerticaBufferedRecords in {@link #write(Collection)}
 */
public class VerticaJdbcDbWriter extends JdbcDbWriter {
  private JdbcSinkConfig config;
  private VerticaDatabaseDialect dbDialect;
  private DbStructure dbStructure;

  private static final Logger log = LoggerFactory.getLogger(VerticaJdbcDbWriter.class);

  VerticaJdbcDbWriter(
          JdbcSinkConfig config,
          VerticaDatabaseDialect dbDialect,
          DbStructure dbStructure) {
    super(config, dbDialect, dbStructure);
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();

    final Map<TableId, VerticaBulkOpsBufferedRecords> bufferByTable = new HashMap<>();
    log.debug("{} records to write", records.size());
    for (SinkRecord record : records) {
      final TableId tableId = destinationTable(record.topic());
      VerticaBulkOpsBufferedRecords buffer = bufferByTable.get(tableId);
      if (buffer == null) {
        buffer = new VerticaBulkOpsBufferedRecords(
            config, tableId, dbDialect, dbStructure,
            connection, record.keySchema(), record.valueSchema());
        bufferByTable.put(tableId, buffer);
      }
      buffer.add(record);
    }
    for (Map.Entry<TableId, VerticaBulkOpsBufferedRecords> entry : bufferByTable.entrySet()) {
      VerticaBulkOpsBufferedRecords buffer = entry.getValue();
      log.debug("Flushing records into {}", entry.getKey());
      buffer.flush();
      buffer.close();
    }
    connection.commit();
    log.debug("{} records committed", records.size());
  }
}
