package jinke.com.hdfsOperation;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

/**
 * Created by tanghanzhuang on 2018/8/3
 */
@Slf4j
public class OperatingFiles {
    static Configuration conf = new Configuration();
    static FileSystem hdfs;

    static {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
        try {
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                Configuration conf = new Configuration();
//                conf.set("fs.default.name", "hdfs://172.16.11.80:9000/");
                conf.set("fs.default.name", "hdfs://172.18.245.121:8020/");
//                Path path = new Path("hdfs://172.16.11.80:9000/");
                Path path = new Path("hdfs://172.18.245.121:8020/");
                hdfs = FileSystem.get(path.toUri(), conf);
                return null;
            });


//            ugi.doAs((PrivilegedExceptionAction <Void> () -> {
//
//                public Void run() throws Exception {
//                    Configuration conf = new Configuration();
//                    conf.set("fs.default.name", "hdfs://172.16.11.80:9000/");
//                    Path path = new Path("hdfs://172.16.11.80:9000/");
//                    hdfs = FileSystem.get(path.toUri(), conf);
//                    return null;
//                }
//            });
        } catch (IOException e) {

        } catch (InterruptedException r) {
        }
    }


    public void getDataFromHDFS(String hdfsPath,String localPath) throws IOException, InterruptedException {
        final String DELIMITER = new String(new byte[]{1});
        final String INNER_DELIMITER = ",";
//        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
//        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
//            Configuration conf = new Configuration();
//                conf.set("fs.defaultFS", "hdfs://172.18.245.121:8020/");
////            conf.set("fs.defaultFS", "hdfs://hdfs://172.16.11.108:8020/");
//                Path path = new Path("hdfs://172.18.245.121:8020/");
////            Path path = new Path("hdfs://172.16.11.108:8020/");
//            hdfs = FileSystem.get(path.toUri(), conf);
//            return null;
//        });

        // 遍历目录下的所有文件
//        BufferedReader br = null;

        InputStream in = null;
        try {

//            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = hdfs.listStatus(new Path(hdfsPath));
            hdfs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
            //            Path path = new Path(localPath);
//            for (FileStatus file : status) {
////                if (!file.getPath().getName().startsWith("part-")) {
////                    continue;
////                }
////                FSDataInputStream inputStream = fs.open(file.getPath());
////                br = new BufferedReader(new InputStreamReader(inputStream));
////                String line = null;
////                while (null != (line = br.readLine())) {
////                    String[] strs = line.split(DELIMITER);
////                    String categoryId = strs[0];
////                    String categorySearchName = strs[9];
////                    if (-1 != categorySearchName.indexOf("0-956955")) {
////                        yhdsxCategoryIdStr += (categoryId + INNER_DELIMITER);
////                    }
////                } //end of while
//                String filePath = String.valueOf(file.getPath());
//                String localFilePath = localPath+"/" + filePath.substring(filePath.lastIndexOf("/"));
//                FileStatus[] status1 = hdfs.listStatus(new Path(filePath));
//                for (FileStatus file1 : status1){
//                    FSDataOutputStream out = hdfs.create(new Path(localFilePath+"/"+String.valueOf(file1.getPath()).substring(String.valueOf(file1.getPath()).lastIndexOf("/"))));
//                    in = hdfs.open(new Path(String.valueOf(file.getPath())));
//                    IOUtils.copyBytes(in, out, 4096, false);
//                }
//
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        return;
    }


    public void reafile() throws IOException {
        String uri = "/user/hive/warehouse/kylin_test.db/douyin_fans";
        if (!hdfs.exists(new Path(uri))) {
            System.out.println("Error ; the file not exists.");
            return;
        }
        InputStream in = null;
        try {
            in = hdfs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public void loadDataToHDFS(String localFilePath,String hdfsFilePath) throws IOException {
        if (hdfs.exists(new Path(hdfsFilePath))) {
            hdfs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath));
            System.out.println("SUCCESS load "+localFilePath+" to "+hdfsFilePath );
        }else {
            System.out.println("The path dosen't exists "+hdfsFilePath);
        }
    }

    public void copyfile() throws IOException {



        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        try {
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                Configuration conf = new Configuration();
//                conf.set("fs.defaultFS", "hdfs://172.16.11.80:9000/");
                conf.set("fs.defaultFS", "hdfs://hdfs://172.16.11.108:8020/");
//                Path path = new Path("hdfs://172.16.11.80:9000/");
                Path path = new Path("hdfs://172.16.11.108:8020/");
                hdfs = FileSystem.get(path.toUri(), conf);
                return null;
            });


//            ugi.doAs((PrivilegedExceptionAction <Void> () -> {
//
//                public Void run() throws Exception {
//                    Configuration conf = new Configuration();
//                    conf.set("fs.default.name", "hdfs://172.16.11.80:9000/");
//                    Path path = new Path("hdfs://172.16.11.80:9000/");
//                    hdfs = FileSystem.get(path.toUri(), conf);
//                    return null;
//                }
//            });
        } catch (IOException e) {

        } catch (InterruptedException r) {
        }

        String localsrc = "E:\\youdu\\download\\file\\parquet\\mission_video\\2.parquet";
        String hdfsDsf = "/test2";
//        String hdfsDsf = "/tmp/test/json";
        Path src = new Path(localsrc);
        Path dst = new Path(hdfsDsf);
        File file = new File(localsrc);

        hdfs.copyFromLocalFile(new Path(file.getPath()), dst);

//        File[] files = file.listFiles();
//        try {
//            for (File file1 : files) {
//                hdfs.copyFromLocalFile(new Path(file1.getPath()), dst);
//                System.out.println("SUCCESS:upload the file to hdfs" + file1.getName());
////                log.info("SUCCESS:upload the file {} to hdfs ", file1.getName());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void deleteHDFSFile(String inpath) throws IOException {
//        File
        Path filePath = new Path(inpath);
        if (hdfs.exists(filePath) && hdfs.isDirectory(filePath)) {
            for (FileStatus status : hdfs.listStatus(filePath)) {
                boolean resull = hdfs.deleteOnExit(status.getPath());
                if (resull) {
                    System.out.println("delete file " + inpath + " success!");
                } else {
                    System.out.println("delete file " + inpath + " failure");
                }

            }
        }
//        return resull;
    }


    public void uploadHdfs() {
        String path = "/mnt/data/parquetPath";
        String hdfspath = "/user/hive/warehouse/mission.db/douyin_fans/";
        File file = new File(path);
        File[] files = file.listFiles();
        for (File parquetFile : files) {
            try {
                hdfs.copyFromLocalFile(new Path(parquetFile.getPath()), new Path(hdfspath));
                parquetFile.delete();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

/*
    public void testSlfj(){
        log.debug("debug");
        log.info("info");
        log.warn("warn");
        log.error("error");
    }*/
}
