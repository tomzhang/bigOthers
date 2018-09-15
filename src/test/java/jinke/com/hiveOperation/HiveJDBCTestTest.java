package jinke.com.hiveOperation;

import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.*;

/**
 * Created by tanghanzhuang on 2018/8/7
 */
public class HiveJDBCTestTest {

    @Test
    public void showDatabases() throws Exception {

        HiveJDBCTest hiveJDBCTest = new HiveJDBCTest();
        hiveJDBCTest.showTables();
    }

    @Test
    public void createPartition() throws SQLException, ClassNotFoundException {
        HiveJDBCTest hiveJDBCTest = new HiveJDBCTest();
        hiveJDBCTest.createPartition();
    }

    @Test
    public void createPartition1() throws SQLException {
/*        HiveJDBCTest hiveJDBCTest = new HiveJDBCTest();
        hiveJDBCTest.createPartition1();*/
        LocalDate localDate = LocalDate.now();
        String dt2 =localDate.toString();
        LocalDate r = localDate.plusDays(1);
        String dt = localDate.getYear() + "-" + localDate.getMonth() + "-" + localDate.getDayOfMonth();
//        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String datetime = r.format(dtf);
        System.out.println(dt);
        System.out.println(dt2);
        System.out.println(datetime);
    }

    @Test
    public void showTables() throws Exception {
        HiveJDBCTest hiveJDBCTest = new HiveJDBCTest();
        hiveJDBCTest.showTables();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        System.out.println(now.format(dtf));
    }
}