import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.StringTokenizer;

public class CustomLineRecordReader extends RecordReader<LongWritable, Text>{

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private LineRecordReader reader;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private Path splitFilePath = null;
    private byte[] endTag = "},".getBytes();
    FSDataInputStream filein = null;
    private boolean stillInChunk = true;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private static final Log LOG = LogFactory.getLog(
            CustomLineRecordReader.class);

    /**
     * From Design Pattern, O'Reilly...
     * This method takes as arguments the map task’s assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
    @Override
    public void initialize(InputSplit split,TaskAttemptContext context) throws IOException {

        FileSplit fileSplit = (FileSplit) split;
        start =  fileSplit.getStart();
        end = start + fileSplit.getLength();
        System.out.println(start);
        System.out.println(end);
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        splitFilePath = fileSplit.getPath();
        filein = fs.open(splitFilePath);
        filein.seek(start);
        if(start!=0){
            readUntilMatch(endTag, false);
        }

    }

    /**
     * From Design Pattern, O'Reilly...
     * Like the corresponding method of the InputFormat class, this reads a
     * single key/ value pair and returns true until the data is consumed.
     */
    // function which generate a record with key value pair
    public boolean nextKeyValue() throws IOException{
        if(!stillInChunk) return false;
        boolean status = readUntilMatch(endTag,true);
        value = new Text();
        value.set(buffer.getData(),0,buffer.getLength());
//        System.out.println(key);
        key = new LongWritable();
        buffer.reset();
        if(!status){
            stillInChunk = false;
        }
        return true;
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i=0;
        int totalByte = 0;
        while(true){
            int nextByte = filein.read();
            if(nextByte == (byte)'"' || nextByte == (byte)'\n' || nextByte == (byte)'\t' || nextByte == (byte)32 || nextByte == (byte)'{')
                continue;
            if(nextByte == -1) return false;
//            if(withinBlock) buffer.write(nextByte);
            if(nextByte == match[i]){
                i++;
                if(i>=match.length){
                    return filein.getPos() < end;
                }
                continue;
            } else {
//                if(nextByte != (byte)'{')
                    buffer.write(nextByte);
                i=0;
            }

        }
    }

    /**
     * From Design Pattern, O'Reilly...
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    /**
     * From Design Pattern, O'Reilly...
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * From Design Pattern, O'Reilly...
     * Like the corresponding method of the InputFormat class, this is an
     * optional method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /**
     * From Design Pattern, O'Reilly...
     * This method is used by the framework for cleanup after there are no more
     * key/value pairs to process.
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
