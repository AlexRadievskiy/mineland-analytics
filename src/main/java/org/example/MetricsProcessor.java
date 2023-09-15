package org.example;

import java.sql.*;
import java.util.Properties;
import java.io.InputStream;

public class MetricsProcessor {

    static String DB_HOST;
    static String USER;
    static String PASS;
    static final String JDBC_URL_TEMPLATE = "jdbc:mysql://%s:3306/ml_analytics?useSSL=false&serverTimezone=UTC";


    static {
        try (InputStream input = MetricsProcessor.class.getClassLoader().getResourceAsStream("database.properties")) {
            Properties prop = new Properties();
            prop.load(input);

            DB_HOST = prop.getProperty("db.host");
            USER = prop.getProperty("db.user");
            PASS = prop.getProperty("db.password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String jdbcUrl = String.format(JDBC_URL_TEMPLATE, DB_HOST);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASS)) {
            processMetrics(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void processMetrics(Connection conn) {
        String selectMetricsQuery = "SELECT id, query FROM core_metrics";
        try (PreparedStatement stmt = conn.prepareStatement(selectMetricsQuery);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int metricId = rs.getInt("id");
                String query = rs.getString("query");

                if (!query.trim().toLowerCase().startsWith("select")) {
                    logError(conn, metricId, "Only SELECT queries are allowed");
                    continue;
                }

                try (PreparedStatement innerStmt = conn.prepareStatement(query);
                     ResultSet innerRs = innerStmt.executeQuery()) {

                    if (innerRs.getMetaData().getColumnCount() > 1) {
                        logError(conn, metricId, "Query returns more than one value");
                        continue;
                    }

                    if (innerRs.next()) {
                        double value = innerRs.getDouble(1);
                        logData(conn, metricId, value);
                    }
                } catch (SQLException e) {
                    logError(conn, metricId, e.getMessage());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logData(Connection conn, int metricId, double value) throws SQLException {
        String insertDataQuery = "INSERT INTO core_metrics_data (date, metric_id, value) VALUES (CURDATE(), ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertDataQuery)) {
            stmt.setInt(1, metricId);
            stmt.setDouble(2, value);
            stmt.executeUpdate();
        }
    }

    private static void logError(Connection conn, int metricId, String error) throws SQLException {
        String insertErrorQuery = "INSERT INTO core_metrics_failed (date, metric_id, error) VALUES (CURDATE(), ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertErrorQuery)) {
            stmt.setInt(1, metricId);
            stmt.setString(2, error);
            stmt.executeUpdate();
        }
    }
}