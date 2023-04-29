package taxi;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YellowTaxiStatsReducer
        extends Reducer<Text, YellowTaxiStatsTuple, Text, Text> {

    private final YellowTaxiStatsTuple result = new YellowTaxiStatsTuple();

    @Override
    public void reduce(Text key, Iterable<YellowTaxiStatsTuple> values, Context context)
            throws IOException, InterruptedException {

        result.setMin(Integer.MAX_VALUE);
        result.setMax(0);
        result.setCount(0);
        long sum = 0;
        double dist_avg_sum = 0;
        double pass_avg_sum = 0;
        double cost_avg_sum = 0;

        for (YellowTaxiStatsTuple val: values){
            if (val.getMin() < result.getMin())
                result.setMin(val.getMin());
            if (result.getMax() == 0 || val.getMax() > result.getMax())
                result.setMax(val.getMax());

            dist_avg_sum += val.getCount() * val.getDistAvg();
            pass_avg_sum += val.getCount() * val.getPassAvg();
            cost_avg_sum += val.getCount() * val.getCostAvg();
            sum += val.getCount();
        }
        result.setCount(sum);
        result.setDistAvg(dist_avg_sum / sum);
        result.setPassAvg(pass_avg_sum / sum);
        result.setCostAvg(cost_avg_sum / sum);

        if (key.toString().equals("trip_distance")){
            String results_str = " min: " + result.getMin() +
                    " max: " + result.getMax() +
                    " count: " + result.getCount() +
                    " avg: " + result.getDistAvg();
            context.write(key, new Text(results_str));
        }

        if (key.toString().equals("passenger_count")){
            String results_str = " avg: " + result.getPassAvg();
            context.write(key, new Text(results_str));
        }

        if (key.toString().equals("total_cost")){
            String results_str = " avg: " + result.getCostAvg();
            context.write(key, new Text(results_str));
        }
    }
}