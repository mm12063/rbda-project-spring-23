import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Map<String, Integer> ColNames = new HashMap<>();
    private YellowTaxiStatsTuple outTuple = new YellowTaxiStatsTuple();

    public void setup(Context context) throws IOException, InterruptedException {
        ColNames.put("pu_date", 0);
        ColNames.put("pu_time", 1);
        ColNames.put("do_date", 2);
        ColNames.put("do_time", 3);
        ColNames.put("htp_am", 4);
        ColNames.put("htp_pm", 5);
        ColNames.put("passenger_count", 6);
        ColNames.put("trip_distance", 7);
        ColNames.put("pu_loc_id", 8);
        ColNames.put("do_loc_id", 9);
        ColNames.put("payment_type", 10);
        ColNames.put("total_cost", 11);
    }

    private String getValue(String col_name, String[] value){
        return value[ColNames.get(col_name)];
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        if (!values[0].equals("pu_date")) {
            try {
                String pu_loc = getValue("pu_loc_id", values);
                context.write(new Text(pu_loc), new IntWritable(1));
            } catch (Exception e) {
                System.out.println("Couldn't read pu_loc_id value from csv");
                System.out.println(e.getMessage());
            }
        }
    }
}
