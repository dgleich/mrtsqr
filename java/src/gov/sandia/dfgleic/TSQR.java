/**
 * A native Java implementation of the Tall-and-skinny QR factorization
 * @author David F. Gleich
 */ 

package gov.sandia.dfgleic;

import java.io.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.Iterator;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.QR;
import no.uib.cipr.matrix.UpperTriangDenseMatrix;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.Type;

import org.apache.log4j.Logger;



/** The main Java driver for the TSQR code
 * mat: the path to the mat file
 * output: the path to the output file
 * blockSize: the block size in the TSQR implementation
 * splitSize: the minimum split size
 * mem: the memory to allocate for each Hadoop job
 * reduceSchedule: 
 */ 
public class TSQR extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(TSQR.class);
    
    public static void main(String args[]) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new TSQR(), args);
    
        System.exit(res);
    }
    
    private static int printUsage() {
        System.out.println("usage: -mat <filepath> [-output <outputpath>]\n" +
        "  [-block_size <int>] [-split_size <int>] [-mem <int]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
    private String getArgument(String arg, String[] args) {
        for (int i=0; i<args.length; ++i) {
            if (arg.equals(args[i])) {
                if (i+1<args.length) {
                    return args[i+1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }
    
    //
    // from
    // http://stackoverflow.com/questions/941272/how-do-i-trim-a-file-extension-from-a-string-in-java
    //
    public static String removeExtension(String s) {

        String separator = System.getProperty("file.separator");

        // Remove the extension.
        int extensionIndex = s.lastIndexOf(".");
        if (extensionIndex == -1)
            return s;

        return s.substring(0, extensionIndex);
    }
    
    public static String getExtension(String s) {

        String separator = System.getProperty("file.separator");

        // Get the extension.
        int extensionIndex = s.lastIndexOf(".");
        if (extensionIndex == -1)
            return s;

        return s.substring(extensionIndex+1);
    }
    
    public int run(String[] args) throws Exception {
        
        if (args.length == 0) {
            return printUsage();
        }
            
        
        String matfile = getArgument("-mat",args);
        if (matfile == null) {
            System.out.println("Required argument '-mat' missing");
            return -1;
        }
        
        String ext=getExtension(matfile);
        String base=removeExtension(matfile);
        
        String outputfile = getArgument("-output",args);
        if (outputfile == null) {
            outputfile = base + "-qrr." + ext;
        }
        
        String reduceSchedule = getArgument("-reduce_schedule",args);
        if (reduceSchedule == null) {
            reduceSchedule = "1";
        }
        
        String blockSize = getArgument("-block_size",args);
        if (blockSize == null) {
            blockSize = "3";
        }
        
        String splitSize = getArgument("-split_size",args);
        
        sLogger.info("Tool name: TSQR");
        sLogger.info(" -mat: " + matfile);
        sLogger.info(" -output: " + outputfile);
        sLogger.info(" -reduce_schedule: " + reduceSchedule);
        sLogger.info(" -block_size: " + blockSize);
        sLogger.info(" -split_size: " + 
            (splitSize == null ? "[Default]" : splitSize));
        
        String stages[] = reduceSchedule.split(",");
        String curinput = matfile;
        String curoutput = outputfile;
        
        
        for (int stage=0; stage<stages.length; ++stage) {
            int numReducers = Integer.parseInt(stages[stage]);
            
            if (stage > 0) {
                curinput = curoutput;
            }
            
            if (stage+1 < stages.length) {
                curoutput = outputfile + "_iter"+(stage+1);
            } else {
                curoutput = outputfile;
            }
            
            // run the iteration
            // TODO make this a separate function?
            JobConf conf = new JobConf(getConf(), TSQR.class);
            DistributedCache.createSymlink(conf);
            conf.setJobName(
                "TSQR.java (" + (stage+1) + "/" + stages.length + ")");
            
            conf.setNumReduceTasks(numReducers);
            //conf.set("mapred.child.java.opts","-Xmx2G");
            if (splitSize != null) {
                conf.set("mapred.minsplit.size", splitSize);
                conf.set("mapreduce.input.fileinputformat.split.minsize", splitSize);
            }
            
            // set the formats
            conf.setInputFormat(SequenceFileInputFormat.class);
            conf.setOutputFormat(SequenceFileOutputFormat.class);
            
            // set the data types
            conf.setOutputKeyClass(TypedBytesWritable.class);
            conf.setOutputValueClass(TypedBytesWritable.class);
            
            if (stage > 0) {
                conf.setMapperClass(IdentityMapper.class);
            } else {
                conf.setMapperClass(TSQRMapper.class);
            }
            conf.setReducerClass(TSQRReducer.class);
            
            FileSystem.get(conf).delete(new Path(curoutput), true);
            FileInputFormat.setInputPaths(conf, new Path(curinput));
            FileOutputFormat.setOutputPath(conf, new Path(curoutput));
            
            sLogger.info("Iteration " + (stage+1) + " of " + stages.length);
            sLogger.info(" - reducers: " + numReducers);
            sLogger.info(" - curinput: " + curinput);
            sLogger.info(" - curoutput: " + curoutput);
            
            JobClient.runJob(conf);
        }
                    
        return 0;
    }
    
    public static class TSQRIteration 
        extends MapReduceBase
    {
        protected int blockSize;
        protected int numColumns;
        protected int currentRow;
        protected Random rand;
        DenseMatrix A;
        
        // this output must be set at some point before close,
        // if there is going to be any output.
        protected OutputCollector<TypedBytesWritable,TypedBytesWritable> output;
        
        public TSQRIteration() {
            this.numColumns = 0;
            this.blockSize = 3;
            this.currentRow = 0;
            this.A = null;
            this.output = null;
            this.rand = new Random();
        }
        
        public TSQRIteration(int blockSize) {
            this();
            this.blockSize = blockSize;
        }
        
        protected TypedBytesWritable randomKey() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            
            TypedBytesOutput out = 
                new TypedBytesOutput(new DataOutputStream(bytes));
            out.writeInt(rand.nextInt(2000000000));
            
            TypedBytesWritable val = 
                new TypedBytesWritable(bytes.toByteArray());
            
            return val;
        }
        
        protected TypedBytesWritable encodeTypedBytes(double array[]) 
            throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            
            TypedBytesOutput out = 
                new TypedBytesOutput(new DataOutputStream(bytes));
                    
            out.writeVectorHeader(array.length);
            for (int i=0; i<array.length; ++i) {
                out.writeDouble(array[i]);
            }
            
            TypedBytesWritable val = 
                new TypedBytesWritable(bytes.toByteArray());
            
            return val;
        }
        
        double readDouble(TypedBytesInput in, Type t) throws IOException {
            if (t == Type.BOOL) {
                boolean b = in.readBool();
                if (b == true) {
                    return 1.;
                } else {
                    return 0.;
                }
            } else if (t == Type.BYTE) {
                byte b = in.readByte();
                return (double)b;
            } else if (t == Type.INT) {
                int i = in.readInt();
                return (double)i;
            } else if (t == Type.LONG) {
                long l = in.readLong();
                return (double)l;
            } else if (t == Type.FLOAT) {
                float f = in.readFloat(); 
                return (double)f;
            } else if (t == Type.DOUBLE) {
                return in.readDouble();
            } else {
                throw new IOException("Type " + t.toString() + " cannot be converted to double ");
            }
        }
        
        protected double[] doubleArrayListToArray(ArrayList<Double> a) {
            double rval[] = new double[a.size()];
            for (int i=0; i<a.size(); ++i) {
                rval[i] = a.get(i).doubleValue();
            }
            return rval;
        }
        
        protected double[] decodeTypedBytesArray(TypedBytesWritable bytes)
            throws IOException {
            
            TypedBytesInput in = 
                new TypedBytesInput(
                    new DataInputStream(
                        new ByteArrayInputStream(bytes.getBytes())));
                        
            Type t = in.readType();
            if (t == Type.VECTOR || t == Type.LIST) {
                if (t == Type.VECTOR) {
                    ArrayList<Double> d = new ArrayList<Double>();
                    int len = in.readVectorHeader();
                    for (int i=0; i<len; ++i) {
                        Type et = in.readType();
                        d.add(new Double(readDouble(in, et)));
                    }
                    return doubleArrayListToArray(d);
                } else {
                    ArrayList<Double> d = new ArrayList<Double>();
                    while (true) {
                        Type et = in.readType();
                        if (et == Type.MARKER) {
                            break;
                        }
                        d.add(new Double(readDouble(in, et)));
                    }
                    return doubleArrayListToArray(d);
                }
            } else {
                return null;
            }
        }
        
        public void compress() {
            /*if (currentRow < A.numRows()) {
                // zero out extra rows
                for (int j=0; j < numColumns; ++j) {
                    for (int i=currentRow; i<A.numRows(); ++i) {
                        A.set(i,j,0.);
                    }
                }
            }*/
                
            // this function should work inplace like Lapack's
            QR qr = QR.factorize(A);
            UpperTriangDenseMatrix R = qr.getR();
            
            A.zero();
            
            // now zero out the lower diagonal
            /*for (int j=0; j<numColumns; ++j) {
                for (int i=j+1; i<numColumns; ++i) {
                    A.set(i,j,0.0);
                }
            }*/
            
            // copy the upper-triangular
            for (int j=0; j<numColumns; ++j) {
                for (int i=0; i<=j; ++i) {
                    A.set(i,j,R.get(i,j));
                }
            }
            
            currentRow = numColumns;
        }
                
        public void collect(TypedBytesWritable key, TypedBytesWritable value) 
            throws IOException {
            double row[] = decodeTypedBytesArray(value);
            if (A == null) {
                numColumns = row.length;
                A = new DenseMatrix(numColumns*blockSize,numColumns);
            } else {
                assert(row.length == numColumns);
            }
            
            // just collect one row at the moment
            assert(currentRow < A.numRows());
            
            for (int i=0; i<row.length; ++i) {
                A.set(currentRow, i, row[i]);
            }
            currentRow ++;
            
            if (currentRow >= A.numRows()) {
                compress();
            }
        }
        
        public void close() throws IOException {
            if (output != null) {
                compress();
                double array[] = new double[numColumns];
                for (int r=0; r<currentRow; ++r) {
                    for (int j=0; j<numColumns; ++j) {
                        array[j] = A.get(r,j);
                    }
                    output.collect(randomKey(), encodeTypedBytes(array));
                }
            }
        }
    }
    
    public static class TSQRMapper 
        extends TSQRIteration
        implements Mapper<TypedBytesWritable, TypedBytesWritable, TypedBytesWritable, TypedBytesWritable> {
        public void map(TypedBytesWritable key, TypedBytesWritable value,
                OutputCollector<TypedBytesWritable,TypedBytesWritable> output,
                Reporter reporter)
            throws IOException {
                
            if (this.output == null) {
                this.output = output;
            }
            
            collect(key,value);
        }
    }
    
    public static class TSQRReducer
        extends TSQRIteration 
        implements Reducer<TypedBytesWritable, TypedBytesWritable, TypedBytesWritable, TypedBytesWritable> {
        public void reduce(TypedBytesWritable key, Iterator<TypedBytesWritable> values,
                OutputCollector<TypedBytesWritable,TypedBytesWritable> output,
                Reporter reporter) 
            throws IOException {
                
            if (this.output == null) {
                this.output = output;
            }
            
            while (values.hasNext()) {
                collect(key,values.next());
            }
        }
    }

}

