package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class MetricsProcessor {

    public void run() {
        try (Connection conn = DatabaseUtils.getConnection()) {
            processMetrics(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (thread.getName().startsWith("mysql-cj-abandoned-connection-cleanup")) {
                    thread.interrupt();
                }
            }
        }
        System.exit(0);
    }

    private void processMetrics(Connection conn) {
        String selectMetricsQuery = "SELECT id, query FROM core_metrics";

        try (PreparedStatement stmt = conn.prepareStatement(selectMetricsQuery);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int metricId = rs.getInt("id");
                String query = rs.getString("query");

                if (!query.trim().toLowerCase().startsWith("select")) {
                    logError(conn, metricId, "Query is not a SELECT statement");
                    continue;
                }

                try (PreparedStatement metricStmt = conn.prepareStatement(query);
                     ResultSet metricRs = metricStmt.executeQuery()) {

                    if (metricRs.next()) {
                        double value = metricRs.getDouble(1);
                        if (metricRs.next()) {
                            logError(conn, metricId, "Query returned more than one value");
                            continue;
                        }
                        logData(conn, metricId, value);
                    } else {
                        logError(conn, metricId, "Query returned no results");
                    }

                } catch (SQLException e) {
                    logError(conn, metricId, e.getMessage());
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void logData(Connection conn, int metricId, double value) throws SQLException {
        String insertDataQuery = "INSERT INTO core_metrics_data (date, metric_id, value) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertDataQuery)) {
            stmt.setTimestamp(1, new Timestamp(new Date().getTime()));
            stmt.setInt(2, metricId);
            stmt.setDouble(3, value);
            stmt.executeUpdate();
        }
    }

    private void logError(Connection conn, int metricId, String error) throws SQLException {
        String insertErrorQuery = "INSERT INTO core_metrics_failed (date, metric_id, error) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertErrorQuery)) {
            stmt.setTimestamp(1, new Timestamp(new Date().getTime()));
            stmt.setInt(2, metricId);
            stmt.setString(3, error);
            stmt.executeUpdate();
        }
    }
}
