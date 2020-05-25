import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <pre>
 *  hdfs文件简单处理
 * </pre>
 *
 * @author Lijin
 * @time 2020/5/23
 */
public class HdfsClient {

    @Test
    public void testClient() throws IOException, URISyntaxException, InterruptedException {
//        Configuration config=new Configuration();
//        config.set("fs.defaultFS","hdfs://bigdata2:9000");
//        FileSystem fs=FileSystem.get(config);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
//        FileSystem fs=FileSystem.get(new URI("hdfs://bigdata2:9000"),config,"bigdata");
        fs.mkdirs(new Path("/data/usr/lijin"));
//        fs.delete(new Path("/data/usr/lijin"),true);
        fs.close();
    }

    @Test
    public void testPutFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        fs.copyFromLocalFile(false, true, new Path("C:\\Users\\Administrator\\Desktop\\笔记.txt"), new Path("/data/usr/lijin"));
        fs.close();
    }

    @Test
    public void testDowlownFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        fs.copyToLocalFile(false, new Path("/data/usr/lijin/笔记.txt"), new Path("E:/"), false);
        fs.close();
    }

    @Test
    public void testDeleteFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        fs.delete(new Path("/data/usr/lijin/笔记.txt"), false);
        fs.close();
    }

    @Test
    public void testChangeFileName() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        fs.rename(new Path("/data/usr/lijin/笔记.txt"), new Path("/data/usr/lijin/note.txt"));
        fs.close();
    }

    @Test
    public void testLookFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path("/data"), true);
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            System.out.println("group：" + fileStatus.getGroup());
            System.out.println("blocksize:" + fileStatus.getBlockSize());
            System.out.println("name：" + fileStatus.getPath().getName());
            System.out.println("permission:" + fileStatus.getPermission());
            System.out.println("len:" + fileStatus.getLen());
            for (BlockLocation blockLocation : fileStatus.getBlockLocations()) {
                for (String host : blockLocation.getHosts()) {
                    System.out.println("host:" + host);
                }
            }
            System.out.println("======== 文件分割  ==========");
        }
        fs.close();
    }

}
