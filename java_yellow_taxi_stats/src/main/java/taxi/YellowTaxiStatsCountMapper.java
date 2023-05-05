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
                String pu_loc = values[CleanedCSVMetaData.getColIdx(ColNames.PU_TAXI_ZONE)];
                pu_loc = clean_location_str(pu_loc);
                String do_loc = values[CleanedCSVMetaData.getColIdx(ColNames.DO_TAXI_ZONE)];
                do_loc = clean_location_str(do_loc);

                String pu_year = values[CleanedCSVMetaData.getColIdx(ColNames.PU_YEAR)];
                if (year.equals(pu_year))
                    context.write(new Text(pu_loc+"___"+do_loc), new IntWritable(1));
            } catch (Exception e) {
                System.out.println("Couldn't read DO_TAXI_ZONE value from csv");
                System.out.println(e.getMessage());
            }
        }
    }
}
