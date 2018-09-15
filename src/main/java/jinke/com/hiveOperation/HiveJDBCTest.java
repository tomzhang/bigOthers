package jinke.com.hiveOperation;

import java.sql.*;
import java.text.DecimalFormat;
import java.time.LocalDate;

/**
 * Created by tanghanzhuang on 2018/8/6
 */
public class HiveJDBCTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://:10000/default";
    private static String user = "";
    private static String password = "";
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void showTables() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        stmt = conn.createStatement();
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        rs.close();
        conn.close();
    }
    public void loadData() throws Exception {
        String filePath = "/home/hadoop/download/datafile/data3.txt";
        String sql = "load data local inpath '" + filePath + "' into table test ";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void  createPartition() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        stmt = conn.createStatement();
        String sql = "alter table testpt add IF NOT EXISTS partition(dt='2018-08-05',hour='14:22:30')";
        stmt.execute(sql);
        System.out.println("Running: " + sql+"success");
        stmt.close();
        conn.close();
    }

/*    public void createPa() {

    }*/

    public void createPartition1() throws SQLException {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://172.16.11.80:10000/test";
        String user = "";
        String password = "";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstm = null;
        ResultSet rs = null;

        LocalDate localDate = LocalDate.now();
        String dt = localDate.toString();
        DecimalFormat df = new DecimalFormat("00");
        String sql1 = "alter table testpt add IF NOT EXISTS partition(dt='?',hour='?')";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            pstm = conn.prepareStatement(sql1);
            for(int hour=0;hour<24;hour++){
//                String sql = "alter table testpt add IF NOT EXISTS partition(dt='"+dt+"',hour='"+df.format(hour)+"')";
//                stmt.execute(sql);
//                log.info("SUCCESS: " + sql);
                pstm.setString(1,dt);
                pstm.setString(2,df.format(hour));
                pstm.addBatch();
            }
            int[] i = pstm.executeBatch();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
//            log.error("FAILURE:"+e);
        } catch (SQLException e) {
            e.printStackTrace();
//            log.error("FAILURE:"+e);
        } finally {
            stmt.close();
            conn.close();
        }
    }
}
