package taxi;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YellowTaxiStatsTop10Reducer
        extends Reducer<NullWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value: values){
            String[] vals = value.toString().split(",");
            repToRecordMap.put(Integer.parseInt(vals[0]), new Text(vals[1]));
            if (repToRecordMap.size() > 6)
                repToRecordMap.remove(repToRecordMap.firstKey());
        }

        for (Map.Entry<Integer, Text> t : repToRecordMap.descendingMap().entrySet()) {
            String t_pair = t.getValue() + ": " + t.getKey();
            context.write(NullWritable.get(), new Text(t_pair));
        }
    }
}