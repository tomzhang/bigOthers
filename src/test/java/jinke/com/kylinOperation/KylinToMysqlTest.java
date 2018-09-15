package jinke.com.kylinOperation;

import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.*;

/**
 * Created by tanghanzhuang on 2018/8/6
 */
public class KylinToMysqlTest {

    @Test
    public void testExport() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
/*        KylinToMysql kylinToMysql = new KylinToMysql();
        kylinToMysql.testExport();*/

    }

    @Test
    public void getConnection() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
/*        KylinToMysql kylinToMysql = new KylinToMysql();
        kylinToMysql.getConnection();*/
    }

    @Test
    public void testExport1() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        KylinToMysql kylinToMysql = new KylinToMysql();
        kylinToMysql.testExport1();
/*        DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse("2018-07-28",df1);
        System.out.println(localDate.getMonthValue());*/
    }
}
