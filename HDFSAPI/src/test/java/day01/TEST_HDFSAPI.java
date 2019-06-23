package day01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TEST_HDFSAPI {
    //ctrl+shift+enter
    @Test
    public void initHDFS() throws IOException {
        //1.创建配置信息对象 ctrl+alt+v 后推前 ctrl+shift+enter 补全
        Configuration conf = new Configuration();
        // 2. 获取文件系统
        //F2快速定位错误 alt+enter:自动找错误
        FileSystem fs = FileSystem.get(conf);
        //3.打印文件系统
        System.out.print(fs.toString());

    }

    /**
     * 文件上传
     */
    @Test
    public void putFileToHDFS() throws IOException, URISyntaxException, InterruptedException {
        //1.创建配置对象信息
        Configuration conf = new Configuration();


        //2.设置部分参数
        conf.set("dfs.replication", "2");

        //3. 找到HDFS的地址
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");


        //4.上传本地文件的路径
        // Path src = new Path("e:/hadoop_test/helloword.txt");
        Path src = new Path("E:\\romantic1128\\135romantic.rar");


        //5.要上传到HDFS的路径
        Path dst = new Path("hdfs://bigdata111:9000/axin");

        //6.以拷贝的方式上传
        fs.copyFromLocalFile(src, dst);

        fs.close();

        System.out.println("ok");

    }

    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.创建配置信息对象
        Configuration conf = new Configuration();
        //2.找到文件系统
        //Ctrl+P
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //3.下载文件
        //boolean delSrc：是否将原文件删除
        //Path src ： 要下载的路径
        //Path dst: 要下载到哪
        //  boolean useRawLocalFileSystem:是否校验文件
        fs.copyToLocalFile(false, new Path("hdfs://bigdata111:9000/axin/hadoop-2.7.2.rar"), new Path("E:\\hadoop_test"), true);

        //4.关闭
        fs.close();

        System.out.println("下载完成");
        //备注：Ctrl+alt+O 可以快速的去除没有用的import
    }

    @Test
    public void mkdirHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //创建目录
        fs.mkdirs(new Path("hdfs://bigdata111:9000/good"));
        fs.close();

        System.out.println("创建完成");

    }

    /**
     * hadoop fs -rm -r /文件
     *
     * @return
     */
    @Test
    public void deleteHDFS() throws URISyntaxException, IOException, InterruptedException {
        //创建配置文件对象
        Configuration conf = new Configuration();
        //user:Linux系统用户
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //true:是否递归删除
        fs.delete(new Path("hdfs://bigdata111:9000/axin/"), true);

        fs.close();

        System.out.println("删除成功");

    }

    /**
     * 查看【文件】名称、权限等
     */
    @Test
    public void readListFiles() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        //遍历迭代器
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            System.out.println("路径 ：" + file.getPath());
            System.out.println("文件名 ：" + file.getPath().getName());
            System.out.println("块大小 ：" + file.getBlockSize());
            System.out.println("权限 ：" + file.getPermission());
            System.out.println("长度 ：" + file.getLen());

            BlockLocation[] locations = file.getBlockLocations();
            //块位置
            for (BlockLocation location : locations) {
                System.out.println("block-offset:" + location.getOffset());
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    //备份所在的主机
                    System.out.println("备份所在的主机：" + host);
                }
                ;
            }
            System.out.println("================================");
        }
    }

    /**
     * 判断是文件还是目录
     */
    @Test
    public void judge() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        FileStatus[] liststatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : liststatus) {
            //判断是否是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录:" + fileStatus.getPath().getName());
            }
        }


    }

    @Test
    public void putFileToHDFSIO() throws URISyntaxException, InterruptedException, IOException {
        //1.创建配置对象信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //3.创建文件输流
        FileInputStream fis = new FileInputStream(new File("E:\\必备软件\\大数据\\TeamViewer.rar"));

        //4.输出路径
        //流上传的方式，需指定文件名
        Path path = new Path("hdfs://bigdata111:9000/TeamViewer.rar");
        FSDataOutputStream fos = fs.create(path);
        //5.流对接
        //java.io.InputStream in,
        // java.io.OutputStream out,
        // int buffSize,
        // boolean close:是否关闭流

        try {
            IOUtils.copyBytes(fis, fos, 4 * 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }


    }

    /**
     * IO读取HDFS到控制台
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void getFileFromHDFSIO() throws URISyntaxException, IOException, InterruptedException {
        //1.创建配置对象信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //3.读取路径
        Path path = new Path("hdfs://bigdata111:9000/TeamViewer.rar");

        //创建输入流
        FSDataInputStream fis = fs.open(path);

        IOUtils.copyBytes(fis, System.out, 4 * 1024, true);

    }


    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        Path path = new Path("hdfs://bigdata111:9000/axin/hadoop-2.7.2.rar");
        FSDataInputStream fis = fs.open(path);

        FileOutputStream fos = new FileOutputStream(new File("E:\\hadoop_test\\AAA1"));

        //读取这个文件的第1块
        byte[] buf = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        fis.close();
        fos.close();


    }

    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        Path path = new Path("hdfs://bigdata111:9000/axin/hadoop-2.7.2.rar");
        FSDataInputStream fis = fs.open(path);

        FileOutputStream fos = new FileOutputStream(new File("E:\\hadoop_test\\AAA2"));

        //读取第二块
        //定位偏移量/offset/游标/（第一块的尾巴，第二块的开头）
        fis.seek(128 * 1024 * 1024);

        //6.流对接
        IOUtils.copyBytes(fis, fos, 1024);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

        //两个文件合到一块 cmd type A2 >> A1
    }


    /**
     * 一致性flush
     *
     * @throws IOException
     */
    @Test
    public void writeFile() throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fos = fs.create(new Path("E:\\hadoop_test\\helloword.txt"));

        //3.写数据
        fos.write("2019-04-12".getBytes());
        fos.flush();
        fos.close();


    }


    //Ctrl+Alt+L 格式化
}
