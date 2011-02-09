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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

import org.apache.hadoop.io.BytesWritable;

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
  implements RecordReader<BytesWritable, BytesWritable> {        

  // reference to the logger
  private static final Log LOG = 
    LogFactory.getLog(FixedLengthRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private InputStream in;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  private int recordLength;
  private int recordKeyStartAt;
  private int recordKeyEndAt;
  private int recordKeyLength;
  private CompressionCodec codec;
  private Decompressor decompressor;
  
  private byte[] recordBytes;

  // our record key 
  private BytesWritable recordKey = null;

  // the record value
  private BytesWritable recordValue = null; 
  
  public FixedLengthRecordReader(FileSplit split, 
                                 Configuration conf) throws IOException {
    // the size of each fixed length record
    this.recordLength = FixedLengthInputFormat.getRecordLength(conf);
    
    // the start position for each key
    this.recordKeyStartAt = FixedLengthInputFormat.getRecordKeyStartAt(conf);
    
    // the end position for each key
    this.recordKeyEndAt = FixedLengthInputFormat.getRecordKeyEndAt(conf);
    
    // record key length (add 1 because the start/end points are INCLUSIVE)
    this.recordKeyLength = recordKeyEndAt - recordKeyStartAt + 1;
    
    // log some debug info
    LOG.info("FixedLengthRecordReader: recordLength="+this.recordLength);
    LOG.info("FixedLengthRecordReader: " + 
        (this.recordKeyStartAt != -1 ? 
        		(" KEY-START-AT=" + this.recordKeyStartAt + 
        		 " KEY-END-AT=" + this.recordKeyEndAt) : 
        		 " NO-CUSTOM-KEY-START/END SPECIFIED, KEY will be record " +
        		 "position in InputSplit"));
                 
    start = split.getStart();
    end = split.getLength() + start;
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(conf);
    codec = compressionCodecs.getCodec(file);
    
    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(conf);
    fileIn = fs.open(file);
    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK); 
        in = (InputStream)cIn;
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn;
      } else {
        in = (InputStream)codec.createInputStream(fileIn, decompressor);
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      in = (InputStream)fileIn;
      filePosition = fileIn;
    }
    
    if (start != 0) {
      // seek to the start of the next record.
      long mod = start % this.recordLength;
      long dist = mod != 0 ? 
        (this.recordLength - (start % this.recordLength))
        : 0;
      LOG.info("FixedLengthRecordReader: " + 
        "Starting at offset " + start + " seeking " + dist + 
        " bytes to next record.");
      filePosition.seek(start+dist);
      start += dist;
    }
    this.pos = start;
  }
  
  public BytesWritable createKey() {
      return new BytesWritable(new byte[this.recordKeyLength]);
  }
  
  public BytesWritable createValue() {
      return new BytesWritable(new byte[this.recordLength]);
  }
  
  private boolean isCompressedInput() {
    return (codec != null);
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
  
  public synchronized long getPos() throws IOException {
    return pos;
  }
  
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  @Override
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / 
          (float)(end-start));
    } 
  }
  
 
  /** Read the next record in the input stream. 
   * @param record an array of recordLength bytes.
   * */
  private void readRecord(byte[] record) throws IOException {
    int totalRead = 0; // total bytes read
    int totalToRead = recordLength; // total bytes we need to read

    // while we still have record bytes to read
    while(totalRead != recordLength) {
      // read in what we need
      int read = this.in.read(record, totalRead, totalToRead);

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
  }
  
  @Override
  public synchronized boolean next(BytesWritable key, BytesWritable value)
    throws IOException {
    
    while (getFilePosition() < end) {
      this.readRecord(value.getBytes());
      
      if (recordKeyStartAt != -1 && recordKeyEndAt != -1) {
      	key.set(recordValue.getBytes(), this.recordKeyStartAt, this.recordKeyLength);
      	
      // otherwise do the default action, (key is record position in the split)
      } else {
      	// default is that the the Key is the position the record started at
        byte[] posKey = toBytes(pos);
      	key.set(posKey,0,posKey.length);
      }
      
      pos += recordLength;
      
      return true;
    }
    
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
