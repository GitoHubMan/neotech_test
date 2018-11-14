package com.neotech.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class DBHelper {
    private Properties properties;
    private final String ROOT_URL_PROP_NAME = "db.root.url";
    public static final String URL_PROP_NAME = "db.url";
    public static final String USER_PROP_NAME = "db.user";
    public static final String PASSWORD_PROP_NAME = "db.password";

    private final String DATABASE_PROP_NAME = "db.database";
    public static final String TABLE_PROP_NAME = "db.table";
    public static final String INSERT_PARAM_NAME = "-p";

    private final String INIT_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS %s";
    private final String INIT_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s(Id BIGINT PRIMARY KEY AUTO_INCREMENT, timestamp timestamp not null);";

    private static final String propsFilePath = "src/main/resources/com/neotech/db.properties";

    public DBHelper() {
        initProperties();
        initDB();
    }

    private void initProperties(){
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(propsFilePath)) {
            properties.load(fis);
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
    }


    private void initDB(){
        String url = properties.getProperty(ROOT_URL_PROP_NAME);
        String user = properties.getProperty(USER_PROP_NAME);
        String password = properties.getProperty(PASSWORD_PROP_NAME);
        String sql = String.format(INIT_DATABASE_SQL, properties.getProperty(DATABASE_PROP_NAME));

        //create database
        try (Connection con = DriverManager.getConnection(url, user, password);
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.execute();
        } catch (SQLException e) {
            System.err.println("Failed to create database["+properties.getProperty(DATABASE_PROP_NAME)+"]. Error: " + e.getMessage());
        }

        url = String.format(properties.getProperty(URL_PROP_NAME), properties.getProperty(DATABASE_PROP_NAME));
        properties.setProperty(URL_PROP_NAME, url);
        sql = String.format(INIT_TABLE_SQL, properties.getProperty(TABLE_PROP_NAME));

        //create test table
        try (Connection con = DriverManager.getConnection(url, user, password);
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.execute();
        } catch (SQLException e) {
            System.err.println("Failed to create test table["+properties.getProperty(TABLE_PROP_NAME)+"]. Error: " + e.getMessage());
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
