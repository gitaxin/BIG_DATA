package compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 21:41 2019/7/28
 */
public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //压缩方法
        //compress("E:\\test_input\\web.txt","org.apache.hadoop.io.compress.GzipCodec");

        //解压缩
        decompress("E:\\test_input\\web.txt.gz",".txt");

    }

    /**
     * 压缩方法
     */
    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(new File(filename));

        //通过反射 找到编/解码的类
        Class codeClass = Class.forName(method);

        //通过反射工具类找到编码器对象，& conf配置
        @SuppressWarnings("unchecked")
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

        CompressionOutputStream cos = codec.createOutputStream(fos);
        //流COPY
        IOUtils.copyBytes(fis,cos,8*1024*1024,false);

        cos.close();
        fos.close();
        fis.close();



    }


    /**
     * 解压缩方法
     */
    private static void decompress(String filename, String decodec) throws IOException {

        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        CompressionCodec codec = factory.getCodec(new Path(filename));

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

        FileOutputStream fos = new FileOutputStream(new File(filename + decodec));

        IOUtils.copyBytes(cis,fos,8*1024*1024,false);

        fos.close();
        cis.close();


    }
}
