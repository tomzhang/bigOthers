package jinke.com.kylinOperation;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Created by tanghanzhuang on 2018/8/6
 */
public class KylinJDBCTestTest {

    @Test
    public void testConnected() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        KylinJDBCTest kylinJDBCTest=new KylinJDBCTest();
        kylinJDBCTest.testConnected();
    }
}