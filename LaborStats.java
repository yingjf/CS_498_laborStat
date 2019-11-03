import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;











import org.apache.hadoop.io.WritableComparable;

// >>> Don't Change
public class LaborStats extends Configured implements Tool {

    public static void main(String[] args) throws Exception {            
        int res = ToolRunner.run(new Configuration(), new LaborStats(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/project/tmp");
        fs.delete(tmpPath, true);
        
        Job jobA = Job.getInstance(this.getConf(), "Labor Stats Area");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapOutputKeyClass(YearMonthArea.class);
        jobA.setMapOutputValueClass(SalaryWorkerNumWritable.class);

        jobA.setMapperClass(LaborStatsAreaMap.class);
        jobA.setReducerClass(LaborStatsAreaReduce.class);

        FileInputFormat.setInputPathFilter(jobA, RegexFilter.class);
        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(LaborStats.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Labor Stats Time");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(DoubleWritable.class);

        jobB.setMapOutputKeyClass(YearMonthArea.class);
        jobB.setMapOutputValueClass(SalaryWorkerNumWritable.class);

        jobB.setMapperClass(LaborStatsTimeMap.class);
        jobB.setReducerClass(LaborStatsTimeReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(LaborStats.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class LaborStatsAreaMap extends Mapper<Object, Text, YearMonthArea, SalaryWorkerNumWritable> {
        YearMonthArea keyYearMonthArea = new YearMonthArea();
        SalaryWorkerNumWritable wageEmplyerNum = new SalaryWorkerNumWritable();   
        PrintWriter writer; // logger

        @Override
        protected void setup(Context context) {
            try{
            	writer = new PrintWriter(new FileOutputStream(new File("/tmp/tryFile.txt"), true /* append = true */)); 
//                writer = new PrintWriter("/tmp/tryFile.txt", "UTF-8");
            } catch (IOException e) {
               // do something
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            boolean writeAlready = true;
            String line = value.toString();
            String[] linedata = line.split("\\s+");
            // Read from the second line of the file
            if (linedata[0].compareTo("series_id") != 0) {
            	// Analyze series_id
            	String seriesId = linedata[0]; // First string of line is the series id
            	char charEnd = seriesId.charAt(seriesId.length()-1); // End of the series id represents the data type
            	int dataType = charEnd-'0'; // Convert the char to int
            	String areacode = seriesId.substring(0, seriesId.length()-1); // the areacode to be stored into the key
            	// Obtain the year value
            	int year = Integer.parseInt(linedata[1]);
            	// Obtain the month value
            	int month = Integer.parseInt(linedata[2].substring(1).replaceAll("^[0]", ""));
            	// Set the key
            	keyYearMonthArea.set(year, month, areacode);
            	// Set the values
            	// dataType 1 is the All Employees, In Thousands
            	if (dataType == 1)  {
            		try {
//            			writer.println(line);
            			int workerNumbers = (int) (Double.parseDouble(linedata[3]) * 1000);
                		wageEmplyerNum.set(0.0, workerNumbers);
            		}
            		catch (Exception e) {
            			// Some files do not have any data for column 3 (e.g. Michigan)
//                		writer.println(line);
//                		writer.flush();
                		return;
            		}
            	}
            	// dataType 6 is the Average hourly Earnings, In Dollars
            	if (dataType == 6)  {
            		double salary = Double.parseDouble(linedata[3]);
            		wageEmplyerNum.set(salary, 0); 	
//            		writer.println(Double.toString(salary));
            	}	
            }
              
            // write context
            context.write(keyYearMonthArea, wageEmplyerNum);
//            writer.close();
        }
        
    }

    public static class LaborStatsAreaReduce extends Reducer<YearMonthArea, SalaryWorkerNumWritable, Text, Text> {
        @Override
        public void reduce(YearMonthArea key, Iterable<SalaryWorkerNumWritable> values, Context context) throws IOException, InterruptedException {
        	YearMonthArea keyYearMonthArea = new YearMonthArea();
            SalaryWorkerNumWritable wageEmplyerNum = new SalaryWorkerNumWritable();
            // Loop through the values and find salary and workerNum values
            for (SalaryWorkerNumWritable val : values) {
                if (val.getSalary() != 0.0) {
                	wageEmplyerNum.setSalary(val.getSalary());
                }
                else if (val.getWorkerNum() != 0) {
                	wageEmplyerNum.setWorkerNum(val.getWorkerNum());
                }
            }
            // If both the salary and workerNum are given for the same area code, output them together
            if (wageEmplyerNum.getSalary() != 0.0 && wageEmplyerNum.getWorkerNum() != 0.0) {
            	String stateCode = (key.getAreacode()).substring(3, 5); // the areacode to be stored into the key
                String stringsKey = Integer.toString(key.getYear()) + "_" + Integer.toString(key.getMonth()) + "_" + stateCode;
                Text valKay = new Text(stringsKey);
                String stringsVal = Double.toString(wageEmplyerNum.getSalary()) + "_" +  Integer.toString(wageEmplyerNum.getWorkerNum());
                Text val = new Text(stringsVal);
                context.write(valKay, val);
            	// Write to test
//                context.write(new Text(key.getAreacode()), new DoubleWritable(wageEmplyerNum.salary));
//                context.write(new Text(key.getAreacode()), new DoubleWritable((double) wageEmplyerNum.workerNumbers));

            }
        }
    }
    
    public static class LaborStatsTimeMap extends Mapper<Text, Text, YearMonthArea, SalaryWorkerNumWritable> {
    	
    	YearMonthArea keyYearMonthArea = new YearMonthArea();
    	SalaryWorkerNumWritable wageSumEmplyerNum = new SalaryWorkerNumWritable();   

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
        	// Convert the key strings to the numbers
        	String keyString = key.toString();
        	String[] partsKey = keyString.split("_");
            int year = Integer.parseInt(partsKey[0]);
            int month = Integer.parseInt(partsKey[1]);
            String state = partsKey[2];
            
            // Convert the val strings to the numbers
            String valString = value.toString();
        	String[] partsVal = valString.split("_");
            double wage = Double.parseDouble(partsVal[0]);
            int employerNum = Integer.parseInt(partsVal[1]);
            
            // Set output wageSumEmplyerNum based on given val data
        	double totalWages = wage * employerNum;
        	wageSumEmplyerNum.setSalary(totalWages);
        	wageSumEmplyerNum.setWorkerNum(employerNum);
        	
        	// Set output key based on given key data
        	keyYearMonthArea.set(year, month, state);

            // write context
        	// Remove the month 13 averaged data
        	if (keyYearMonthArea.getMonth() != 13) {
        		context.write(keyYearMonthArea, wageSumEmplyerNum);
        	}
        }
        
    }

    public static class LaborStatsTimeReduce extends Reducer<YearMonthArea, SalaryWorkerNumWritable, Text, DoubleWritable> {
        @Override
        public void reduce(YearMonthArea key, Iterable<SalaryWorkerNumWritable> values, Context context) throws IOException, InterruptedException {
        	Double sumWage, sumWorkerNum, avgWage, medianWage;
        	sumWage = 0.0;
        	sumWorkerNum = 0.0;
        	List<Double> avgWageList = new ArrayList<Double>();
        	
            // Loop through the values and find salary and workerNum values
            for (SalaryWorkerNumWritable val : values) {
            	sumWage += val.getSalary();
            	sumWorkerNum += val.getWorkerNum();
            	// Add into the array list to calculate the median value
            	double avg = val.getSalary()/val.getWorkerNum();
            	avgWageList.add(avg);
            }
            avgWage = sumWage / sumWorkerNum;
            
            // Obtain the median salary
            Collections.sort(avgWageList);
            int middle = avgWageList.size()/2;
            if (avgWageList.size() % 2 == 1) {
            	medianWage = avgWageList.get(middle);
            } else {
            	medianWage = (avgWageList.get(middle-1) + avgWageList.get(middle)) / 2.0;
            }
            
            // Write
            String tempString  = "State " + key.getAreacode() + " average wage in year " + Integer.toString(key.getYear()) + " in month " + Integer.toString(key.getMonth()) + ":";
            context.write(new Text(tempString), new DoubleWritable(avgWage));
            tempString  = "State " + key.getAreacode() + " median wage in year " + Integer.toString(key.getYear()) + " in month " + Integer.toString(key.getMonth()) + ":";
            context.write(new Text(tempString), new DoubleWritable(medianWage));

        }
    }
    
    public static class RegexFilter extends Configured implements PathFilter {

        @Override
        public boolean accept(Path path) {
        	if ((path.toString()).contains("sm.data.")) {
        		return true;
        	}
        	else {
        		return false;
        	}
        }
    }
    
    public static class YearMonthArea implements WritableComparable<YearMonthArea> {
    	private int year;
    	private int month;
    	private String areacode;
    	
        @Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((areacode == null) ? 0 : areacode.hashCode());
			result = prime * result + month;
			result = prime * result + year;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			YearMonthArea other = (YearMonthArea) obj;
			if (areacode == null) {
				if (other.areacode != null)
					return false;
			} else if (!areacode.equals(other.areacode))
				return false;
			if (month != other.month)
				return false;
			if (year != other.year)
				return false;
			return true;
		}

		public YearMonthArea() {
        	set(-1,-1,"");
        }
        
    	public YearMonthArea(int year, int month, String areacode) {
    		set(year, month, areacode);
    	}
    	
    	public int getYear() {
            return year;
        }
    	
    	public int getMonth() {
            return month;
        }
    	
    	public String getAreacode() {
            return areacode;
        }
    	
    	public void set(int year, int month, String areacode) {
    		this.year = year;
    		this.month = month;
    		this.areacode = areacode;
    	}
    	
        public void readFields(DataInput in) throws IOException {
        	year = in.readInt();
        	month = in.readInt();
        	areacode = in.readUTF();
        }

        public void write(DataOutput out) throws IOException {
        	 out.writeInt(year);
        	 out.writeInt(month);
        	 out.writeUTF(areacode);
        }
        
        public int compareTo(YearMonthArea tp) {
            if (Integer.compare(this.year, tp.year) == 0) {
            	if (Integer.compare(this.month, tp.month) == 0) {
            		return this.areacode.compareTo(tp.areacode);	
            	}
            	else 
            		return Integer.compare(this.month, tp.month);
            }
            else
            	return Integer.compare(this.year, tp.year);
        }
    	
    }

    public static class SalaryWorkerNumWritable implements Writable {
        private double salary;
        private int workerNumbers;
     
        public SalaryWorkerNumWritable() {
        	salary = 0.00;
        	workerNumbers = 0;
        }
     
        public SalaryWorkerNumWritable(double salary, int workerNumbers) {
            this.salary = salary;
            this.workerNumbers = workerNumbers;
        }
        
    	public void set(double salary, int workerNumbers) {
    		this.salary = salary;
    		this.workerNumbers = workerNumbers;
    	}
     
        public void setSalary(double salary) {
            this.salary = salary;
        }
        
        public void setWorkerNum(int workerNumbers) {
        	this.workerNumbers = workerNumbers;
        }
        
        public double getSalary() {
            return salary;
        }
        
        public int getWorkerNum() {
            return workerNumbers;
        }
        
        public void readFields(DataInput in) throws IOException {
        	salary = in.readDouble();
        	workerNumbers = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
        	 out.writeDouble(salary);
        	 out.writeInt(workerNumbers);
        }
    }
}