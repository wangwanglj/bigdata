import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <pre>
 *  hdfs文件io处理
 * </pre>
 *
 * @author Lijin
 * @time 2020/5/23
 */
public class HdfsIO {

    /**
     * 上传文件
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");
        //本地文件系统
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        InputStream inputStream = localFs.open(new Path("F:\\百度云\\01_尚硅谷大数据技术之Hadoop（加密播放）\\2.资料\\hadoop-2.7.2.zip.json"));
//        InputStream inputStream = new FileInputStream(new File("C:\\\\Users\\\\Administrator\\\\Desktop\\\\更新.txt"));
        OutputStream outputStream = fs.create(new Path("/hadoop-2.7.2.zip.json"), true);
        IOUtils.copyBytes(inputStream, outputStream, configuration, true);

        fs.close();
        localFs.close();
    }

    /**
     * 下载文件
     */
    @Test
    public void dowlownFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        //文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");

        //输入流
        InputStream inputStream = fileSystem.open(new Path("/data/usr/lijin/test.txt"));
        //输出流
        OutputStream outputStream = new FileOutputStream(new File("E://test.txt"));
        //复制
        IOUtils.copyBytes(inputStream, outputStream, configuration, true);

        //回收资源
        fileSystem.close();
    }

    /**
     * 分块读取文件，第一部分
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");


        //输入流
        InputStream inputStream = fs.open(new Path("/hadoop-2.7.2.zip.json"));
        //输出流
        OutputStream outputStream = new FileOutputStream(new File("E://hadoop-2.7.2.zip.part1"));

        /**
         * 读取缓存
         */
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            inputStream.read(bytes);
            outputStream.write(bytes);
        }

        //关闭资源
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
        fs.close();

    }

    /**
     * 读取第二部分文件
     */
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata2:9000"), configuration, "bigdata");

        //流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hadoop-2.7.2.zip.json"));
        OutputStream outputStream = new FileOutputStream(new File("E:/hadoop-2.7.2.zip.part2"));

        //定位输入数据位置
        fsDataInputStream.seek(128 * 1024 * 1024);

        //拷贝
        IOUtils.copyBytes(fsDataInputStream, outputStream, configuration, true);

        fileSystem.close();
    }
}
