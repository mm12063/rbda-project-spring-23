package taxi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;

public class YellowTaxiStats {

    private static String leftZeroPad(int num){
        StringBuilder padded_str = new StringBuilder();
        if (num < 10) {
            padded_str.append("0000").append(num);
        } else if (num < 100) {
            padded_str.append("000").append(num);
        } else if (num < 1000) {
            padded_str.append("00").append(num);
        } else {
            padded_str.append(num);
        }
        return padded_str.toString();
    }

    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set("mapred.textoutputformat.separator", ",");
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(YellowTaxiStats.class);
//        job.setJobName("YellowTaxiStats");
//        job.setNumReduceTasks(1);
//
        int MAX_FILES = 102; // Just a subset of the files as dataproc kept crashing under the strain of all 103 files
        String main_dir = args[1];

////        for (int file_num = 0; file_num <= MAX_FILES; file_num++) {
////            String input = main_dir + "/part-m-" + leftZeroPad(file_num);
////            MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, YellowTaxiStatsMapper.class);
////        }
//
//        String input_file = args[1];
//        MultipleInputs.addInputPath(job, new Path(input_file), TextInputFormat.class, YellowTaxiStatsMapper.class);
//
//        String out_path_str = args[2];
//        Path out_path = new Path(out_path_str);
//        FileUtils.deleteDirectory(new File(out_path_str));
////        job.setCombinerClass(taxi.YellowTaxiStatsReducer.class);
//        job.setReducerClass(YellowTaxiStatsReducer.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, out_path);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(YellowTaxiStatsTuple.class);
//        job.waitForCompletion(true);

        int YEAR_START = 2015;
        int YEAR_END = 2021;

        for (int year = YEAR_START; year <= YEAR_END; year++){

            // Job
            // Count the number of times passengers are picked up at certain location id
            String counter_out_path_str = args[2] +"_"+ year;
            Path counter_out_path = new Path(counter_out_path_str);
            FileUtils.deleteDirectory(new File(counter_out_path_str));
            Configuration conf_counter = new Configuration();
            conf_counter.set("mapred.textoutputformat.separator", ",");
            Job job_counter = Job.getInstance(conf_counter);
            job_counter.setJarByClass(YellowTaxiStats.class);
            job_counter.setJobName("Job Name: YellowTaxiStatsCounter for Top 10 for "+year);
            job_counter.setNumReduceTasks(1);

            for (int file_num = 0; file_num <= MAX_FILES; file_num++) {
                String input = main_dir + "/part-m-" + leftZeroPad(file_num);
                MultipleInputs.addInputPath(job_counter, new Path(input), TextInputFormat.class, YellowTaxiStatsCountMapper.class);
            }

            job_counter.setCombinerClass(YellowTaxiStatsCounterReducer.class);
            job_counter.setReducerClass(YellowTaxiStatsCounterReducer.class);
            job_counter.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job_counter, counter_out_path);
            job_counter.setOutputKeyClass(Text.class);
            job_counter.setOutputValueClass(IntWritable.class);
            job_counter.waitForCompletion(true);


            // Job
            // Return the top 10 pick up locations
            Configuration conf_top_10 = new Configuration();
            conf_top_10.set("mapred.textoutputformat.separator", ",");
            Job job_top_10 = Job.getInstance(conf_top_10, "YellowTaxiStatsTop10 "+year);
            job_top_10.setJarByClass(YellowTaxiStats.class);
            job_top_10.setMapperClass(YellowTaxiStatsTop10Mapper.class);
            job_top_10.setReducerClass(YellowTaxiStatsTop10Reducer.class);
            job_top_10.setNumReduceTasks(1);
            job_top_10.setOutputKeyClass(NullWritable.class);
            job_top_10.setOutputValueClass(Text.class);
            // Use the output file from the previous job as the input to this job
            FileInputFormat.addInputPath(job_top_10, new Path(counter_out_path_str));
            FileOutputFormat.setOutputPath(job_top_10, new Path(counter_out_path_str+"_top10"));
            job_top_10.waitForCompletion(true);

        }








//        // Job
//        // Count the number of times passengers are picked up at certain location id
//        String pu_loc_counter_file = args[2];
//        Path pu_loc_counter = new Path(pu_loc_counter_file);
//        FileUtils.deleteDirectory(new File(pu_loc_counter_file));
//        Configuration conf_counter = new Configuration();
//        conf_counter.set("mapred.textoutputformat.separator", ",");
//        Job job_loc_id_count = Job.getInstance(conf_counter);
//        job_loc_id_count.setJarByClass(YellowTaxiStats.class);
//        job_loc_id_count.setJobName("Job Name: YellowTaxiStatsLocIDCounter");
//        job_loc_id_count.setNumReduceTasks(1);
//
//        for (int file_num = 0; file_num <= MAX_FILES; file_num++) {
//            String input = main_dir + "/part-m-" + leftZeroPad(file_num);
//            MultipleInputs.addInputPath(job_loc_id_count, new Path(input), TextInputFormat.class, YellowTaxiStatsCountZonesMapper.class);
//        }
//
//        job_loc_id_count.setCombinerClass(YellowTaxiStatsCountZonesReducer.class);
//        job_loc_id_count.setReducerClass(YellowTaxiStatsCountZonesReducer.class);
//        job_loc_id_count.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job_loc_id_count, pu_loc_counter);
//        job_loc_id_count.setOutputKeyClass(Text.class);
//        job_loc_id_count.setOutputValueClass(IntWritable.class);
//        job_loc_id_count.waitForCompletion(true);

    }
}
