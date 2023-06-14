package taxi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowTaxiStatsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String year = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        year = context.getConfiguration().get("year");
    }

    private String clean_location_str(String in){
        in = in.replace(" ","_");
        in = in.replace("/","__");
        in = in.replace("'","");
        in = in.replace("-","");
        return in;
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        if (!values[0].equals("pu_date")) {
            try {
                String payment = values[CleanedCSVMetaData.getColIdx(ColNames.PAYMENT_TYPE)];
                String pu_year = values[CleanedCSVMetaData.getColIdx(ColNames.PU_YEAR)];
                if (year.equals(pu_year))
                    context.write(new Text(payment), new IntWritable(1));
            } catch (Exception e) {
                System.out.println("Couldn't read DO_TAXI_ZONE value from csv");
                System.out.println(e.getMessage());
            }
        }
    }
}
