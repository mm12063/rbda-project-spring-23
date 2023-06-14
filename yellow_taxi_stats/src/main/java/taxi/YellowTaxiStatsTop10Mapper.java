package taxi;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsTop10Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        String location = values[0];
        String total = values[1];

        repToRecordMap.put(Integer.parseInt(total), new Text(location));
        if (repToRecordMap.size() > 5)
            repToRecordMap.remove(repToRecordMap.firstKey());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Text> t : repToRecordMap.entrySet()) {
            String t_pair = t.getKey() + "," + t.getValue();
            context.write(NullWritable.get(), new Text(t_pair));
        }
    }
}
