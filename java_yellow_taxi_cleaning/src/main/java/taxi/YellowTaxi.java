package taxi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;


//import java.io.File;
import java.io.File;
import java.net.URI;
//import java.nio.file.Files;
//import java.nio.file.Paths;

public class YellowTaxi {

    private static String leftZeroPad(int num){
        StringBuilder padded_str = new StringBuilder();
        if (num < 10)
            padded_str.append("0").append(num);
        else
            padded_str.append(num);

        return padded_str.toString();
    }

    public static void main(String[] args) throws Exception {
//        if (args.length != 3) {
//            System.err.println("Usage: taxi.YellowTaxi <input path> <input path> <output path>");
//            System.exit(-1);
//        }

        int YEAR_ST = 2015;
        int YEAR_END = 2021;
        int MONTH_ST = 1;
        int MONTH_END = 12;

        File f = new File("./flag.txt");
        if(f.exists()) {
            YEAR_ST = 2020;
            YEAR_END = 2020;
            MONTH_ST = 4;
            MONTH_END = 4;
        }

        String DS_1_LOC = args[1];
        String DS_2_DIR = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(YellowTaxi.class);
        job.setJobName("YellowTaxi Parquet");
        job.setNumReduceTasks(0);

        try {
            job.addCacheFile(new URI(DS_1_LOC));
        } catch (Exception e) {
            System.out.println("Couldn't add the file to cache");
            System.exit(1);
        }

        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        String out_path_str = args[3];
        FileUtils.deleteDirectory(new File(out_path_str));
        Path out_path = new Path(out_path_str);

        for (int year = YEAR_ST; year <= YEAR_END; year++) {
            for (int month = MONTH_ST; month <= MONTH_END; month++) {
                String input = DS_2_DIR + "/" + year + "/yellow_tripdata_" + year + "-" + leftZeroPad(month) + ".parquet";
                System.out.println(input);
                MultipleInputs.addInputPath(job, new Path(input), ParquetInputFormat.class, YellowTaxiMapper.class);
            }
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out_path);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
