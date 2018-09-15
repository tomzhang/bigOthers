package jinke.com.hdfsOperation;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by tanghanzhuang on 2018/8/3
 */
public class OperatingFilesTest {

/*
    @Test
    public void reafile() throws IOException {

        OperatingFiles operatingFiles = new OperatingFiles();
        operatingFiles.reafile();

    }
*/
    @Test
    public void copyfile() throws IOException {
        OperatingFiles operatingFiles = new OperatingFiles();
        operatingFiles.copyfile();
    }

    @Test
    public void testSlfj() {/*
        OperatingFiles operatingFiles = new OperatingFiles();
        operatingFiles.testSlfj();*/

        LocalDate localDate = LocalDate.now();
        List<String> list =new ArrayList<>();
        DecimalFormat df = new DecimalFormat("00");
        list.add(localDate.getYear()+"-"+df.format(localDate.getMonthValue()+1)+"-01");
        list.add(localDate.getYear()+"-"+df.format(localDate.getMonthValue()+1)+"-15");
        System.out.println(df.format(localDate.getMonthValue()+2));
    }

    @Test
    public void getYHDSXCategoryIdStr() throws IOException, InterruptedException {
        OperatingFiles operatingFiles = new OperatingFiles();
        String hdfsPath = "/apps/hive/warehouse/mission.db/douyin_fans";
        String localPath = "E:\\opt\\missionData";
        operatingFiles.getDataFromHDFS(hdfsPath,localPath);
    }

    @Test
    public void deleteHDFSFile() throws IOException {
        String basePath = "/apps/hive/warehouse/mission.db/douyin_fans/dt=" ;
        String startDateStr = "2018-08-21";
        String endDateStr = "2018-09-14";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startDate = LocalDate.parse(startDateStr,dtf);
        LocalDate endDate = LocalDate.parse(endDateStr,dtf);
        LocalDate empDate = startDate;
        while (empDate.isBefore(endDate) || empDate.isEqual(endDate)){
            String filePath = basePath+dtf.format(empDate);
            OperatingFiles operatingFiles = new OperatingFiles();
            operatingFiles.deleteHDFSFile(filePath);
            System.out.println("All file in " +filePath+"had been delete !");
            empDate = empDate.plusDays(1);
        }

    }

    @Test
    public void loadDataToHDFS() throws IOException {
        OperatingFiles operatingFiles = new OperatingFiles();
        String baseHDFSPath = "/apps/hive/warehouse/mission.db/douyin_fans/";
        String baselocalPath = "E:\\opt\\missionData\\new_douyin_fans";

        String startDateStr = "2018-08-21";
        String endDateStr = "2018-09-14";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startDate = LocalDate.parse(startDateStr,dtf);
        LocalDate endDate = LocalDate.parse(endDateStr,dtf);
        LocalDate empDate = startDate;

        File file = new File(baselocalPath);
        File[] files = file.listFiles();

        for (File file1 : files){
//            String fileDate = dtf.format(empDate);

            if (file1.isDirectory()){
                String filename = file1.getName().substring(file1.getName().lastIndexOf("=")+1);
                empDate = LocalDate.parse(filename,dtf);
                if (empDate.isBefore(endDate.plusDays(1)) && empDate.isAfter(startDate.minusDays(1))){
                    File[] parquetFiles = file1.listFiles();
                    for (File file2:parquetFiles){
                        if (file2.getName().endsWith(".parquet")){
                            String inPath = file2.getPath();
                            String fileName = inPath.substring(inPath.lastIndexOf("\\")+1);
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                            String fileTimeStr = fileName.substring(0, fileName.indexOf("_"));
                            LocalDateTime fileTime = LocalDateTime.parse(fileTimeStr, formatter1);
                            String newFilePath = "dt=" + formatter.format(fileTime);

                            String localPath = file2.getPath();
                            String HDFSPATH = baseHDFSPath + newFilePath;
                            operatingFiles.loadDataToHDFS(localPath,HDFSPATH);
                        }
                    }
                }

            }
        }


    }
}