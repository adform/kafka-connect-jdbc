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

import io.confluent.connect.jdbc.dialect.VerticaMergeDatabaseDialect;

/**
 * Just change JdbcDbWriter to VerticaJdbcDbWriter in {@link #initWriter()}
 */
public class VerticaSinkTask extends JdbcSinkTask {

  @Override
  void initWriter() {
    VerticaMergeDatabaseDialect dialect = new VerticaMergeDatabaseDialect(config);
    this.dialect = dialect;
    final DbStructure dbStructure = new DbStructure(dialect);
    writer = new VerticaJdbcDbWriter(config, dialect, dbStructure);
  }
}
