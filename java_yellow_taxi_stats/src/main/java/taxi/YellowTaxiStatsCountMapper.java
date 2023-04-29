package taxi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final YellowTaxiStatsTuple outTuple = new YellowTaxiStatsTuple();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        if (!values[0].equals("pu_date")) {
            try {
                String pu_loc = values[CleanedCSVMetaData.getColIdx(ColNames.PU_LOC_ID)];
                context.write(new Text(pu_loc), new IntWritable(1));
            } catch (Exception e) {
                System.out.println("Couldn't read pu_loc_id value from csv");
                System.out.println(e.getMessage());
            }
        }
    }
}
