package taxi;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsMapper extends Mapper<LongWritable, Text, Text, YellowTaxiStatsTuple> {
    private final YellowTaxiStatsTuple outTuple = new YellowTaxiStatsTuple();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        if (!values[0].equals("pu_month")) { // If it's the column name row, ignore it totally
            try {
                double trip_dist = Double.parseDouble(values[CleanedCSVMetaData.getColIdx(ColNames.TRIP_DISTANCE)]);
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
                int pass_count = Integer.parseInt(values[CleanedCSVMetaData.getColIdx(ColNames.PASSENGER_COUNT)]);
                outTuple.setPassAvg(pass_count);
                context.write(new Text("passenger_count"), outTuple);
            } catch (Exception e) {
                System.out.println("Couldn't read passenger_count value from csv");
                System.out.println(e.getMessage());
            }

            try {
                double total_cost = Double.parseDouble(values[CleanedCSVMetaData.getColIdx(ColNames.TOTAL_COST)]);
                outTuple.setCostAvg(total_cost);
                context.write(new Text("total_cost"), outTuple);
            } catch (Exception e) {
                System.out.println("Couldn't read total_cost value from csv");
                System.out.println(e.getMessage());
            }

        }
    }
}
