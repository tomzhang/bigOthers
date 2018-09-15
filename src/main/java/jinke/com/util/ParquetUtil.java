package jinke.com.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * @author qiuchaojie
 * @date 2018/7/28
 */
public class ParquetUtil {


    /**
     * 获取操作parquet文件writer
     * @author qiuchaojie
     * @param parquetPath 文件路径
     * @param messageType
     * @return
     */
    public static ParquetWriter<Group> getParquetWriter(String parquetPath, MessageType messageType) {
        ParquetWriter<Group> parquetWriter = null;
        Path path = new Path(parquetPath);
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(messageType, configuration);
        try {
            parquetWriter = new ParquetWriter<>(path, configuration, writeSupport);
        } catch (IOException e) {

        }
        return parquetWriter;
    }
}
