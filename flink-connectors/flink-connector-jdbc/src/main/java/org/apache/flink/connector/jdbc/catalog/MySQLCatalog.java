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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for MySQL. */
@Internal
@SuppressWarnings("deprecation")
public class MySQLCatalog extends AbstractJdbcCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("performance_schema");
                    add("metrics_schema");
                    add("sys");
                    add("mysql");
                }
            };

    protected MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl,
            String additionalParams
    ) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl, additionalParams);
    }

    protected MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl
    ) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl, "");
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> mysqlDatabases = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

            PreparedStatement ps = conn.prepareStatement("SELECT schema_name FROM information_schema.schemata;");

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                if (!builtinDatabases.contains(dbName.toLowerCase())) {
                    mysqlDatabases.add(rs.getString(1));
                }
            }

            return mysqlDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        List<String> tables = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT table_name FROM information_schema.tables "
                            + "WHERE table_type = 'BASE TABLE' AND table_schema = ? "
                            + "ORDER BY table_name");

            stmt.setString(1, databaseName);

            ResultSet rstables = stmt.executeQuery();

            while (rstables.next()) {
                tables.add(rstables.getString(1));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String dbUrl = baseUrl + tablePath.getDatabaseName() + "?" + additionalParams;
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(metaData, tablePath.getDatabaseName(), tablePath.getObjectName());

            PreparedStatement ps =
                    conn.prepareStatement(String.format("SELECT * FROM %s LIMIT 1;", tablePath.getObjectName()));

            ResultSetMetaData rsmd = ps.getMetaData();

            String[] names = new String[rsmd.getColumnCount()];
            DataType[] types = new DataType[rsmd.getColumnCount()];

            List<String> pkColumns = new ArrayList<>();
            primaryKey.ifPresent(pk -> pkColumns.addAll(pk.getColumns()));

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                names[i - 1] = rsmd.getColumnName(i);
                types[i - 1] = fromJDBCType(rsmd, i);

                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls && pkColumns.contains(names[i - 1])) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);

            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    public static final String MYSQL_TINYINT = "TINYINT";
    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    public static final String MYSQL_SMALLINT = "SMALLINT";
    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    public static final String MYSQL_INT = "INT";
    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    public static final String MYSQL_INTEGER = "INTEGER";
    public static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    public static final String MYSQL_BIGINT = "BIGINT";
    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String MYSQL_FLOAT = "FLOAT";
    public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    public static final String MYSQL_DOUBLE = "DOUBLE";
    public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    public static final String MYSQL_NUMERIC = "NUMERIC";
    public static final String MYSQL_NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    public static final String MYSQL_DECIMAL = "DECIMAL";
    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MYSQL_BOOLEAN = "BOOLEAN";
    public static final String MYSQL_DATE = "DATE";
    public static final String MYSQL_TIME = "TIME";
    public static final String MYSQL_DATETIME = "DATETIME";
    public static final String MYSQL_CHAR = "CHAR";
    public static final String MYSQL_VARCHAR = "VARCHAR";
    public static final String MYSQL_TEXT = "TEXT";
    public static final String MYSQL_BINARY = "BINARY";
    public static final String MYSQL_VARBINARY = "VARBINARY";
    public static final String MYSQL_BLOB = "BLOB";

    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        int displaySize = metadata.getColumnDisplaySize(colIndex);

        switch (mysqlType) {
            case MYSQL_TINYINT:
            case MYSQL_TINYINT_UNSIGNED:
                if (additionalParams.contains("tinyInt1isBit=false")) {
                    return DataTypes.INT();
                }
                return displaySize > 1 ? DataTypes.INT() : DataTypes.BOOLEAN();
            case MYSQL_SMALLINT:
            case MYSQL_INT:
            case MYSQL_INTEGER:
            case MYSQL_MEDIUMINT:
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_BIGINT:
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_FLOAT:
            case MYSQL_FLOAT_UNSIGNED:
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
            case MYSQL_DOUBLE_UNSIGNED:
                return DataTypes.DOUBLE();
            case MYSQL_NUMERIC:
            case MYSQL_DECIMAL:
            case MYSQL_NUMERIC_UNSIGNED:
            case MYSQL_DECIMAL_UNSIGNED:
                if (precision >= DecimalType.MIN_PRECISION) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
            case MYSQL_BOOLEAN:
                return DataTypes.BOOLEAN();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                if (precision < TimeType.MIN_PRECISION || precision > TimeType.MAX_PRECISION) {
                    precision = TimeType.DEFAULT_PRECISION;
                }
                return DataTypes.TIME(precision);
            case MYSQL_DATETIME:
                if (precision < TimestampType.MIN_PRECISION || precision > TimestampType.MAX_PRECISION) {
                    precision = TimestampType.DEFAULT_PRECISION;
                }
                return DataTypes.TIMESTAMP(precision);
            case MYSQL_CHAR:
                // return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
                // return DataTypes.VARCHAR(precision);
            case MYSQL_TEXT:
                return DataTypes.STRING();
            case MYSQL_BINARY:
            case MYSQL_VARBINARY:
            case MYSQL_BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support MySQL type '%s' yet", mysqlType));
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        return tables.contains(tablePath.getObjectName());
    }
}
