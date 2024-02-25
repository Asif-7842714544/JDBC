import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.logging.Logger;

public class Test2 {
    private static final Logger logger = Logger.getLogger(Test1.class.getName());

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/movies_api";
        String username = "root";
        String password = "Casifbasha@98480";
        String outputDirectory = "C:\\Users\\user\\Desktop\\java\\";


        Connection conn = null;
        PreparedStatement stmt = null;
        PreparedStatement countstmt = null;
        ResultSet rs = null;
        ResultSet countrs = null;
        try {
            Instant start = Instant.now();
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);

            String sql = "SELECT * FROM movies_api.persons";
            String countquery = "select count(*) as count from movies_api.persons";
            stmt = conn.prepareStatement(sql);
            countstmt = conn.prepareStatement(countquery);
            rs = stmt.executeQuery();
            countrs = countstmt.executeQuery();
            int recCount=0;

            while (countrs.next()) {
                recCount = countrs.getInt("count");
            }
            System.out.println("reccount: " + recCount);

            int batchSize = 2000;
            int count = 0;
            int totalrecordcount = 0;
            int fileCount = (int) Math.ceil((double) recCount / batchSize);
            int fileSeq = 1;
            FileWriter writer = new FileWriter(outputDirectory + "data_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + fileCount + "_" + fileSeq + ".csv");
            writer.append("Account Number,Account Name\n");
            while (rs.next()) {
//                uuid, first_name, last_name, birthday
                String uuid = rs.getString("uuid");
                String first_name = rs.getString("first_name");
                String last_name = rs.getString("last_name");
                String birthday = rs.getString("birthday");
                writer.append(uuid);
                writer.append(",");
                writer.append(first_name);
                writer.append(",");
                writer.append(last_name);
                writer.append(",");
                writer.append(birthday);
                writer.append("\n");
                count++;
                totalrecordcount++;
                if (count % batchSize == 0) {
                    writer.append("Total count: " + count);
                    writer.flush();
                    writer.close();

                    logger.info("Written " + count + " records to data_" + fileCount + ".csv");
//                    fileCount++;
                    writer = new FileWriter(outputDirectory + "data_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + fileCount + "_" + fileSeq + ".csv");
                    fileSeq++;
                    count = 0;
                    writer.append("Account Number,Account Name\n");
                }
            }
            writer.append("Total count: " + count);
            writer.flush();
            writer.close();
            Instant end = Instant.now();
            logger.info("Total time  " + Duration.between(start, end));
            logger.info("Export completed. Total records: " + totalrecordcount);
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
            logger.severe("Error occurred: " + e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.severe("Error closing resources: " + e.getMessage());
            }
        }
    }
}
