/**
 * @author David F. Gleich
 */ 

package gov.sandia.dfgleic;

import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobContext;


  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map.
   */
  public class NullInputFormat implements InputFormat<Text, BytesWritable> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public InputSplit[] getSplits(JobConf job, int inputSplits) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      Path outDir = FileOutputFormat.getOutputPath(job);
      String numMapsConfig = null;
      try {
          Class c = Class.forName("org.apache.hadoop.mapreduce.MRJobConfig");
          Field f = c.getField("NUM_MAPS");
          numMapsConfig = (String)f.get(null);
      } catch (Throwable e) {
          numMapsConfig = "mapred.map.tasks";
      }
      int numSplits = job.getInt(numMapsConfig, 1);
      for(int i=0; i < numSplits; ++i) {
        result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
                                  (String[])null));
      }
      InputSplit[] rval = {result.get(0)};
      rval = result.toArray(rval); 
      return rval;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader implements RecordReader<Text, BytesWritable> {
      Path name;
      BytesWritable value = new BytesWritable();
      
      public RandomRecordReader(Path p, JobConf job) {
        name = p;
      }

      public Text createKey() {
        return new Text();
      }

      public BytesWritable createValue() {
        return new BytesWritable();
      }
      
      public boolean next(Text key, BytesWritable value) {
        if (name != null) {
          key.set(name.getName());
          value.set(this.value);
          name = null;
          return true;
        } else {
          return false;
        }
      }
        
      
      public void close() {}

      public float getProgress() {
        return 0.0f;
      }
      public long getPos() {
        return 0;
      }
    }

    @Override
    public RecordReader<Text, BytesWritable> getRecordReader(
          InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new RandomRecordReader(((FileSplit) split).getPath(), job);
    }
  }
