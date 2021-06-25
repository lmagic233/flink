/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MySQLTablePath {
    private static final String DEFAULT_MYSQL_DATABASE_NAME = "default";
    
    private final String mysqlDatabaseName;
    private final String mysqlTableName;

    public MySQLTablePath(String mysqlDatabaseName, String mysqlTableName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(mysqlDatabaseName));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(mysqlTableName));

        this.mysqlDatabaseName = mysqlDatabaseName;
        this.mysqlTableName = mysqlTableName;
    }
    
    public static MySQLTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path != null && path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new MySQLTablePath(path[0], path[1]);
        } else {
            return new MySQLTablePath(DEFAULT_MYSQL_DATABASE_NAME, flinkTableName);
        }
    }

    public static String toFlinkTableName(String database, String table) {
        return new MySQLTablePath(database, table).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", mysqlDatabaseName, mysqlTableName);
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    public String getMysqlSchemaName() {
        return mysqlDatabaseName;
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MySQLTablePath that = (MySQLTablePath) o;
        return Objects.equals(mysqlDatabaseName, that.mysqlDatabaseName)
                && Objects.equals(mysqlTableName, that.mysqlTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mysqlDatabaseName, mysqlTableName);
    }
}
