package outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:33 2019/8/2
 */
public class FilterRecordWriter extends RecordWriter<Text,NullWritable> {

    FSDataOutputStream itstarOut = null;
    FSDataOutputStream otherOut = null;


    /**
     * 获取配置信息，从而得到FS
     * @param job
     * @throws IOException
     */
    public FilterRecordWriter(TaskAttemptContext job) throws IOException {

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path itstartPath = new Path("E:\\test_input\\itstarlog\\BC\\itstart.log");
        Path otherPath = new Path("E:\\test_input\\itstarlog\\BC\\other.log");
        itstarOut = fs.create(itstartPath);
        otherOut = fs.create(otherPath);
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String string = text.toString();
        if(string.contains("itstar")){
            itstarOut.write(string.getBytes());
        }else{
            otherOut.write(string.getBytes());

        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (itstarOut != null) {
            itstarOut.close();
        }
        if (otherOut != null) {
            otherOut.close();
        }
    }
}
