import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsMapper extends Mapper<LongWritable, Text, Text, YellowTaxiStatsTuple> {
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
        if (!values[0].equals("pu_date")) { // If it's the column name row, ignore it totally
            try {
                double trip_dist = Double.parseDouble(getValue("trip_distance", values));
                outTuple.setMin(trip_dist);
                outTuple.setMax(trip_dist);
                outTuple.setDistAvg(trip_dist);
                outTuple.setCount(1);
                context.write(new Text("trip_distance"), outTuple);
            } catch (Exception e) {
                System.out.println("Couldn't read trip_distance value from csv");
                System.out.println(e.getMessage());
            }

            try {
                int pass_count = Integer.parseInt(getValue("passenger_count", values));
                outTuple.setPassAvg(pass_count);
                context.write(new Text("passenger_count"), outTuple);
            } catch (Exception e) {
                System.out.println("Couldn't read passenger_count value from csv");
                System.out.println(e.getMessage());
            }

            try {
                double total_cost = Double.parseDouble(getValue("total_cost", values));
                outTuple.setCostAvg(total_cost);
                context.write(new Text("total_cost"), outTuple);
            } catch (Exception e) {
                System.out.println("Couldn't read total_cost value from csv");
                System.out.println(e.getMessage());
            }

        }
    }
}
