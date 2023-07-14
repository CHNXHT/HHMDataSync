package com.idata.hhmdataconnector.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class tableUtil {
    public static void createMySQLTable(SparkSession spark, String tableName, StructType schema) throws SQLException {
        String[] columns = schema.fieldNames();
        String[] columnTypes = new String[columns.length];

        for (int i = 0; i < columns.length; i++) {
            StructField field = schema.fields()[i];
            String columnName = field.name();
            String columnType = oracleToMySQLDataType(field.dataType());
            columnTypes[i] = columnName + " " + columnType;
        }

        String createTableQuery =
                String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, String.join(",\n", columnTypes));

        createTable(createTableQuery,"jdbc:mysql://172.16.16.32:3306/contradiction?useSSL=false","root","idata@2023");

    }

    public static void  createTable(String createTableQuery, String jdbcUrl, String username, String password) throws SQLException {
        // Establish JDBC connection
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

        // Create statement and execute the query
        Statement statement = connection.createStatement();
        statement.executeUpdate(createTableQuery);

        // Close the statement and connection
        statement.close();
        connection.close();
    }

    public static void  deleteCFdata(String createTableQuery, String jdbcUrl, String username, String password) throws SQLException {
        // Establish JDBC connection
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

        // Create statement and execute the query
        Statement statement = connection.createStatement();
        statement.executeUpdate(createTableQuery);

        // Close the statement and connection
        statement.close();
        connection.close();
    }


    public static String oracleToMySQLDataType(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return "INT";
        } else if (dataType instanceof LongType) {
            return "BIGINT";
        } else if (dataType instanceof FloatType) {
            return "FLOAT";
        } else if (dataType instanceof DoubleType) {
            return "DOUBLE";
        } else if (dataType instanceof StringType) {
            return "VARCHAR(255)";
        } else if (dataType instanceof TimestampType){
            return "DATETIME";
        } else if (dataType instanceof DecimalType) {
            return "BIGINT";
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
}
