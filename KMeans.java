/*V
 * @author Himank Chaudhary
 */

import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;


class DoublePair implements WritableComparable<DoublePair>{
	private DoubleWritable first;
	private DoubleWritable second;
	
	public DoublePair() {
		set(new DoubleWritable(), new DoubleWritable());
	}
	
	public DoublePair(double first, double second) {
		set(new DoubleWritable(first), new DoubleWritable(second));
	}
	
	public void set(DoubleWritable first, DoubleWritable second) {
		this.first = first;
		this.second = second;
	}
	
	public DoubleWritable getFirst() {
		return first;
	}
	
	public Double getFirstDouble() {
		return new Double(first.toString());
	}
	
	public DoubleWritable getSecond() {
		return second;
	}
	
	public Double getSecondDouble() {
		return new Double(second.toString());
	}
    private double pow2(double n){
        return n*n;
    }
    
    public double euDis(DoublePair p2){
        double x1 = this.getFirstDouble();
        double y1 = this.getSecondDouble();
        double x2 = p2.getFirstDouble();
        double y2 = p2.getSecondDouble();
        return Math.sqrt(pow2(x2-x1)+pow2(y2-y1));

    }
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof DoublePair) {
			DoublePair tp = (DoublePair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public int compareTo(DoublePair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}

@SuppressWarnings("deprecation")
public class KMeans {
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-00000";
    public static String DATA_FILE_NAME = "/data.txt";
    public static String JOB_NAME = "KMeans";
    public static String SPLITTER = "\t| ";
    public static String DLI ="(,| |\t|\n)";
    //public static String SPLITTER = "\n";
    //public static List<Double> mCenters = new ArrayList<Double>();
    public static List<DoublePair> mCenters = new ArrayList<DoublePair>();
    public static long numPoints;
    public static long numCetners;
    public static int maxIter=1;


    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, DoublePair, DoublePair> {
        @Override
        public void configure(JobConf job) {
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    try {
                        // Read the file split by the splitter and store it in
                        // the list
                        while ((line = cacheReader.readLine()) != null) {
                            Scanner sc = new Scanner(line).useDelimiter(DLI);
                            double x = sc.nextDouble();
                            double y = sc.nextDouble();
                            DoublePair point = new DoublePair(x,y);
                            mCenters.add(point);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<DoublePair, DoublePair> output,
                Reporter reporter) throws IOException {
            String line = value.toString();
            String[] raw = line.split(DLI);

            DoublePair point = new DoublePair(Double.parseDouble(raw[0]),
                                          Double.parseDouble(raw[1]));


            
            int nearest_center = 0;
            double minDis=mCenters.get(0).euDis(point);
            double temDis;
            // Find the minimum center from a point
            for (int i = 1; i<mCenters.size();i++){
                temDis = mCenters.get(i).euDis(point); 
                if(temDis<minDis){
                    minDis = temDis;
                    nearest_center = i;
                }
            }
            // Emit the nearest center and the point
            output.collect(mCenters.get(nearest_center),point);
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<DoublePair, DoublePair, DoublePair, IntWritable> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         */
        @Override
        public void reduce(DoublePair key, Iterator<DoublePair> values,
                OutputCollector<DoublePair, IntWritable> output, Reporter reporter)
                throws IOException {


            double newX=0;
            double newY=0;
            int no_elements = 0;
            while (values.hasNext()) {
                DoublePair d = values.next();
                newX += d.getFirstDouble();
                newY += d.getSecondDouble();
                ++no_elements;
            }

            // We have new center now
            newX /= no_elements;
            newY /= no_elements;

            DoublePair newCenter = new DoublePair(newX,newY);

            // Emit new center and point
            output.collect(newCenter, new IntWritable(no_elements));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        numPoints = Integer.parseInt(args[2]);
        numCetners = Integer.parseInt(args[3]);
        maxIter = Integer.parseInt(args[4]);

        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;

        // Reiterating till the convergence
        int iteration = 0;
        boolean isdone = false;
        while (isdone == false && iteration <maxIter) {
             
            JobConf conf = new JobConf(KMeans.class);
            conf.setNumReduceTasks(1);
            if (iteration == 0) {
                Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } else {
                Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
                //upload the file to hdfs. Overwrite any existing copy.
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            }
            conf.setJobName(JOB_NAME);
            conf.setMapOutputKeyClass(DoublePair.class);
            conf.setMapOutputValueClass(DoublePair.class);
            conf.setOutputKeyClass(DoublePair.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf,
                    new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(conf, new Path(output));

            JobClient.runJob(conf);

            Path ofile = new Path(output + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    fs.open(ofile)));
            List<DoublePair> centers_next = new ArrayList<DoublePair>();
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split(DLI);
                double x = Double.parseDouble(sp[0]);
                double y = Double.parseDouble(sp[1]);
                DoublePair c = new DoublePair(x,y);
                centers_next.add(c);
                line = br.readLine();
            }
            br.close();

            String prev;
            if (iteration == 0) {
                prev = input + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(
                    fs1.open(prevfile)));
            List<DoublePair> centers_prev = new ArrayList<DoublePair>();
            String l = br1.readLine();
            while (l != null) {
                String[] sp1 = l.split(DLI);
                double x1 = Double.parseDouble(sp1[0]);
                double y1 = Double.parseDouble(sp1[1]);
                DoublePair c1 = new DoublePair(x1,y1);
                centers_prev.add(c1);
                l = br1.readLine();
            }
            br1.close();

            // Sort the old centroid and new centroid and check for convergence
            // cokkkkkkkkkkkkkkkkkkkkkkkkkkkkndition
            //for (
            Collections.sort(centers_next);
            Collections.sort(centers_prev);

            double err = 0;
            Iterator<DoublePair> it = centers_prev.iterator();
            for (DoublePair d : centers_next) {
                DoublePair temp = it.next();
                err += temp.euDis(d);
            }
            if(err <=0.05){
                isdone = true;
            }
            ++iteration;
            again_input = output;
            output = OUT + System.nanoTime();
        }
    }
}
