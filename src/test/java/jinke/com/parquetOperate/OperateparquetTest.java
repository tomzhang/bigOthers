package jinke.com.parquetOperate;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class OperateparquetTest {

    @Test
    public void readParquet() throws IOException {
        Operateparquet operateparquet = new Operateparquet();
        operateparquet.readParquet();
    }

    @Test
    public void parquetReader() {
    }

    @Test
    public void showParquetData() throws IOException {
        String inpath = "E:\\opt\\missionData\\douyin_fans\\dt=2018-08-23\\20180823235500_fans.parquet";
        Operateparquet operateparquet = new Operateparquet();
        operateparquet.showParquetData(inpath);
    }
}