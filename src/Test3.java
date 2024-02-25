import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Test3 {
    private static final Logger logger = Logger.getLogger(Test1.class.getName());
    private static final int NUM_THREADS = 4; // Number of threads for parallel processing
    private static final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/movies_api";
        String username = "root";
        String password = "Casifbasha@98480";
        String outputDirectory = "C:\\Users\\user\\Desktop\\java\\";

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        FileWriter[] writers = new FileWriter[NUM_THREADS]; // Declare writers outside the try block

        try {
            Instant start = Instant.now();
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false); // Enable manual commit

            String sql = "SELECT * FROM movies_api.persons";
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE); // Set fetch size for streaming
            rs = stmt.executeQuery();

            for (int i = 0; i < NUM_THREADS; i++) {
                writers[i] = new FileWriter(outputDirectory + "data_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_thread_" + i + ".csv.z");
                writers[i].append("Account Number,Account Name\n");
            }

            int count = 0;
            while (rs.next()) {
                final int threadIndex = count % NUM_THREADS;
                final String uuid = rs.getString("uuid");
                final String first_name = rs.getString("first_name");
                final String last_name = rs.getString("last_name");
                final String birthday = rs.getString("birthday");

                FileWriter writer = writers[threadIndex]; // Get the writer for the current thread
                executor.submit(() -> {
                    try {
                        synchronized (writer) {
                            writer.append(uuid).append(",").append(first_name).append(",").append(last_name).append(",").append(birthday).append("\n");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                count++;
            }

            executor.shutdown();
            while (!executor.isTerminated()) {
                // Wait for all tasks to finish
            }

            conn.commit(); // Commit the transaction

            Instant end = Instant.now();
            logger.info("Total time: " + Duration.between(start, end));
            logger.info("Export completed. Total records: " + count);

        } catch (ClassNotFoundException | SQLException | IOException e) {
            if (conn != null) {
                try {
                    conn.rollback(); // Rollback in case of exception
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            e.printStackTrace();
            logger.severe("Error occurred: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
                for (FileWriter writer : writers) {
                    writer.close();
                }
            } catch (SQLException | IOException e) {
                e.printStackTrace();
                logger.severe("Error closing resources: " + e.getMessage());
            }
        }
    }
}
