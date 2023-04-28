package taxi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;


import java.net.URI;

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
        System.out.println("Main start");

        String DS_1_LOC = args[1];
        String DS_2_DIR = args[2];

        Configuration conf = new Configuration();
//        conf.set("join.type","inner");
//        conf.set("service.id.file.path", DS_1_LOC);
        Job job = Job.getInstance(conf);
        job.setJarByClass(YellowTaxi.class);
        job.setJobName("YellowTaxi");
        job.setNumReduceTasks(0);

//        try {
//            job.addCacheFile(new URI(DS_1_LOC));
//        } catch (Exception e) {
//            System.out.println("Couldnt add the file to cache");
//            System.exit(1);
//        }

        System.out.println("Main 2");

        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        int YEAR_ST = 2015;
        int YEAR_END = 2015;
        int MONTH_ST = 1;
        int MONTH_END = 1;

        String out_path_str = args[3];
        Path out_path = new Path(out_path_str);

        //        FileUtils.deleteDirectory(new File(out_path_str));


//        String csv_input = "/Users/mitch/Desktop/NYU/classes/rbda/rbda-project-spring-23/CSVs/2020/yellow_tripdata_2020-01-5.csv";
//        MultipleInputs.addInputPath(job, new Path(csv_input), TextInputFormat.class, taxi.YellowTaxiMapper.class);
        System.out.println("Main 3");

        for (int year = YEAR_ST; year <= YEAR_END; year++) {
            for (int month = MONTH_ST; month <= MONTH_END; month++) {
                String input = DS_2_DIR + "/" + year + "/yellow_tripdata_" + year + "-" + leftZeroPad(month) + ".parquet";
                MultipleInputs.addInputPath(job, new Path(input), ParquetInputFormat.class, YellowTaxiMapper.class);
            }
        }

        System.out.println("Main 4");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out_path);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        System.out.println("Main 5");

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
