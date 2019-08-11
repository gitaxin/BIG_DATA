package inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:46 2019/8/2
 */
public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable> {

    //配置、切片、数据加工、Value值
    private Configuration configuration;

    private FileSplit fileSplit;

    private boolean processes = false;

    private BytesWritable value = new BytesWritable();;

    public WholeRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit)inputSplit;
        configuration = taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!processes){
            FSDataInputStream fis;
            //定义缓存区
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            fis = fs.open(path);
            IOUtils.readFully(fis,contents,0,contents.length);


            value.set(contents,0,contents.length);
            fis.close();

            processes = true;
            return true;

        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
       return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processes ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
