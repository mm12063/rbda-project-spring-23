import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

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

        if (args.length != 2) {
            System.err.println("Usage: YellowTaxi <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(YellowTaxi.class);
        job.setJobName("Job Name: YellowTaxi");
        job.setNumReduceTasks(0);

        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        int YEAR_ST = 2015;
        int YEAR_END = 2021;
        int MONTH_ST = 1;
        int MONTH_END = 12;
        String main_dir = args[0];

        for (int year = YEAR_ST; year <= YEAR_END; year++) {
            for (int month = MONTH_ST; month <= MONTH_END; month++) {
                String input = main_dir + "/" + year + "/yellow_tripdata_" + year + "-" + leftZeroPad(month) + ".parquet";
                MultipleInputs.addInputPath(job, new Path(input), ParquetInputFormat.class, YellowTaxiMapper.class);
            }
        }

        String out_path_str = args[1];
        Path out_path = new Path(out_path_str);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out_path);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
