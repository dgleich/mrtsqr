/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * 
 * FixedLengthRecordReader is returned by FixedLengthInputFormat. This reader
 * uses the record length property set within the FixedLengthInputFormat to 
 * read one record at a time from the given InputSplit. This record reader
 * does not support compressed files.<BR><BR>
 * 
 * Each call to nextKeyValue() updates the LongWritable KEY and BytesWritable 
 * VALUE.<BR><BR>
 * 
 * KEY = (BytesWritable) The KEY is the either the record position (Long as a byte array)
 * within the  InputSplit OR the bytes located between the FixedLengthInputFormat.FIXED_RECORD_KEY_START_AT
 * and FixedLengthInputFormat.FIXED_RECORD_KEY_END_AT property values, if set
 * by the caller when the job was configured.<BR><BR>
 * 
 * VALUE = the record itself (BytesWritable)
 * 
 * @see FixedLengthInputFormat
 *
 */
public class FixedLengthRecordReader 
    extends RecordReader<BytesWritable, BytesWritable> {

  // reference to the logger
  private static final Log LOG = 
    LogFactory.getLog(FixedLengthRecordReader.class);

  // the start point of our split
  private long splitStart;

  // the end point in our split
  private long splitEnd; 

  // our current position in the split
  private long currentPosition;

  // the length of a record
  private int recordLength; 
  
  // the start position of a record key
  private int recordKeyStartAt; 
  
  // the end position of a record key
  private int recordKeyEndAt; 
  
  // the record key length
  private int recordKeyLength;

  // reference to the input stream
  private FSDataInputStream fileInputStream;

  // the input byte counter
  private Counter inputByteCounter; 

  // our record key 
  private BytesWritable recordKey = null;

  // the record value
  private BytesWritable recordValue = null; 

  @Override
  public void close() throws IOException {
    if (fileInputStream != null) {
      fileInputStream.close();
    }
  }

  @Override
  public BytesWritable getCurrentKey() throws IOException,
  InterruptedException {
    return recordKey;
  }

  @Override
  public BytesWritable getCurrentValue() 
      throws IOException, InterruptedException {
    return recordValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (splitStart == splitEnd) {
      return (float)0;
    } else {
      return Math.min((float)1.0, (currentPosition - splitStart) / 
          (float)(splitEnd - splitStart));
    } 
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
  throws IOException, InterruptedException {

    // the file input fileSplit
    FileSplit fileSplit = (FileSplit)inputSplit;

    // the byte position this fileSplit starts at within the splitEnd file
    splitStart = fileSplit.getStart();

    // splitEnd byte marker that the fileSplit ends at within the splitEnd file
    splitEnd = splitStart + fileSplit.getLength();

    // the actual file we will be reading from
    Path file = fileSplit.getPath(); 

    // job configuration
    Configuration job = context.getConfiguration(); 

    // check to see if compressed....
    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading"+
              " compressed files");
    }

    // for updating the total bytes read in 
    inputByteCounter = 
      ((MapContext)context).getCounter(FileInputFormat.COUNTER_GROUP, 
          FileInputFormat.BYTES_READ); 
 
    // the size of each fixed length record
    this.recordLength = FixedLengthInputFormat.getRecordLength(job);
    
    // the start position for each key
    this.recordKeyStartAt = FixedLengthInputFormat.getRecordKeyStartAt(job);
    
    // the end position for each key
    this.recordKeyEndAt = FixedLengthInputFormat.getRecordKeyEndAt(job);
    
    // record key length (add 1 because the start/end points are INCLUSIVE)
    this.recordKeyLength = recordKeyEndAt - recordKeyStartAt + 1;
    

    // log some debug info
    LOG.info("FixedLengthRecordReader: SPLIT-START="+splitStart + 
        " SPLIT-END=" +splitEnd + " SPLIT-LENGTH="+fileSplit.getLength() +
        (this.recordKeyStartAt != -1 ? 
        		(" KEY-START-AT=" + this.recordKeyStartAt + 
        		 " KEY-END-AT=" + this.recordKeyEndAt) : 
        		 " NO-CUSTOM-KEY-START/END SPECIFIED, KEY will be record " +
        		 "position in InputSplit"));
 

    // get the filesystem
    final FileSystem fs = file.getFileSystem(job); 

    // open the File
    fileInputStream = fs.open(file); 

    // seek to the splitStart position
    fileInputStream.seek(splitStart);

    // set our current position
    this.currentPosition = splitStart; 
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

	// allocate a key
    if (recordKey == null) {
      recordKey = new BytesWritable(new byte[this.recordKeyLength]);
    }

    // the recordValue to place the record text in
    if (recordValue == null) {
      recordValue = new BytesWritable(new byte[this.recordLength]);
    }
    
    // the byte buffer we will store data in
    byte[] valueBytes = recordValue.getBytes();
    
    // the current position before we start moving forward
    long thisStartingPosition = currentPosition;

    // if the currentPosition is less than the split end..
    if (currentPosition < splitEnd) {

      int totalRead = 0; // total bytes read
      int totalToRead = recordLength; // total bytes we need to read

      // while we still have record bytes to read
      while(totalRead != recordLength) {
        // read in what we need
        int read = this.fileInputStream.read(valueBytes, totalRead, totalToRead);

        /* EOF? this is an error because each 
         * split calculated by FixedLengthInputFormat
         * contains complete records, if we receive 
         * an EOF within this loop, then we have
         * only read a partial record as totalRead != recordLength
         */
        if (read == -1) {
        	throw new IOException("FixedLengthRecordReader, " +
        	        " unexpectedly encountered an EOF when attempting" +
        			" to read in an entire record from the current split");
        }
        
        // read will never be zero, because read is only
        // zero if you pass in zero to the read() call above

        // update our markers
        totalRead += read;
        totalToRead -= read;
      }

      // update our current position and log the input bytes
      currentPosition = currentPosition +recordLength;
      inputByteCounter.increment(recordLength);

      // Determine the KEY value
      // if recordKeyStartAt and recordKeyEndAt are not the defaults (not set)
      // the use that as the key
      if (recordKeyStartAt != -1 && recordKeyEndAt != -1) {
      	recordKey.set(recordValue.getBytes(), this.recordKeyStartAt, this.recordKeyLength);
      	
      // otherwise do the default action, (key is record position in the split)
      } else {
      	// default is that the the Key is the position the record started at
        byte[] posKey = toBytes(thisStartingPosition);
      	recordKey.set(posKey,0,posKey.length);
      }
      
      return true;             
    }

    // nothing more to read....
    return false;
  }

  
  public static byte[] toBytes(long val) {
    byte [] b = new byte[8];
    for(int i=7;i>0;i--) {
      b[i] = (byte)(val);
      val >>>= 8;
    }
    b[0] = (byte)(val);
    return b;
  }
}
