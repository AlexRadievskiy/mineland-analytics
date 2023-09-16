package org.example;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseUtils {

    private static final String JDBC_URL_TEMPLATE = "jdbc:mysql://%s:3306/ml_analytics?useSSL=false&serverTimezone=UTC";
    private static String DB_HOST;
    private static String USER;
    private static String PASS;

    static {
        try (InputStream input = DatabaseUtils.class.getClassLoader().getResourceAsStream("database.properties")) {
            Properties prop = new Properties();
            prop.load(input);

            DB_HOST = prop.getProperty("db.host");
            USER = prop.getProperty("db.user");
            PASS = prop.getProperty("db.password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        String jdbcUrl = String.format(JDBC_URL_TEMPLATE, DB_HOST);
        return DriverManager.getConnection(jdbcUrl, USER, PASS);
    }
}
