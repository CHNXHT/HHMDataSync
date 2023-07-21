package com.idata.hhmdataconnector.utils;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
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

    public static void  deleteTableBeforeInsert(String tableName, String jdbcUrl, String username, String password,String beginTime,String endTime,String timeFiled,String sourceFlag){
        // Establish JDBC connection
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String deleteTableQuery = "";
        if(tableName.equals("t_mediation_case")){
            deleteTableQuery = String.format("delete from "+ tableName+ " where "+ timeFiled +" between '"+ beginTime+"' and '"+endTime+"'"+"and case_source = '"+sourceFlag+"'");
        }else {
            deleteTableQuery = String.format("delete from "+ tableName+ " where "+ timeFiled +" between '"+ beginTime+"' and '"+endTime+"'");
        }

        System.out.println(deleteTableQuery);
        // Create statement and execute the query
        Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            statement.executeUpdate(deleteTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Close the statement and connection
        try {
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

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

    public static void main(String[] args) throws SQLException {
        String beginTime = "2018-01-01";
        String endtime = DateUtil.beginOfDay(DateUtil.yesterday()).toString("yyyy-MM-dd HH:mm:ss");
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd HH:mm:ss");
        deleteTableBeforeInsert("t_mediation_case", DataSource.HHM.getUrl(),"root", DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"create_time","1");
    }
}
