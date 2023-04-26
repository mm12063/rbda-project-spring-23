import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.simple.SimpleGroup;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class YellowTaxiMapper extends Mapper<LongWritable, SimpleGroup, NullWritable, Text> {
    private final Map<String, Integer> ColNames = new HashMap<>();

    public void setup(Context context) throws IOException, InterruptedException {
        ColNames.put("VendorID", 0);
        ColNames.put("tpep_pickup_datetime", 1);
        ColNames.put("tpep_dropoff_datetime", 2);
        ColNames.put("passenger_count", 3);
        ColNames.put("trip_distance", 4);
        ColNames.put("RatecodeID", 5);
        ColNames.put("store_and_fwd_flag", 6);
        ColNames.put("PULocationID", 7);
        ColNames.put("DOLocationID", 8);
        ColNames.put("payment_type", 9);
        ColNames.put("fare_amount", 10);
        ColNames.put("extra", 11);
        ColNames.put("mta_tax", 12);
        ColNames.put("tip_amount", 13);
        ColNames.put("tolls_amount", 14);
        ColNames.put("improvement_surcharge", 15);
        ColNames.put("total_amount", 16);
        ColNames.put("congestion_surcharge", 17);
        ColNames.put("airport_fee", 18);

        String csv_header = "pu_date" + "," +
                "pu_time" + "," +
                "do_date" + "," +
                "do_time" + "," +
                "htp_am" + "," +
                "htp_pm" + "," +
                "passenger_count" + "," +
                "trip_distance" + "," +
                "pu_loc_id" + "," +
                "do_loc_id" + "," +
                "payment_type" + "," +
                "total_cost";

        context.write(NullWritable.get(), new Text(csv_header));
    }

    private int getCol(String col_name) {
        return ColNames.get(col_name);
    }

    private String getValue(String col_name, SimpleGroup value){
        return value.getValueToString(getCol(col_name), 0);
    }

    private String getDateOnly(String timestamp){
        long ts = Long.parseLong(timestamp) / 1000;
        Date date_only = new Date(ts);
        DateFormat f = new SimpleDateFormat("MM-dd-yyyy", Locale.ENGLISH);
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f.format(date_only);
    }

    private String getTimeOnly(String timestamp){
        long ts = Long.parseLong(timestamp) / 1000;
        Date date_only = new Date(ts);
        DateFormat f = new SimpleDateFormat("HH:mm", Locale.ENGLISH);
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f.format(date_only);
    }

    @Override
    public void map(LongWritable key, SimpleGroup value, Context context)
            throws IOException, InterruptedException {

        // High Traffic Period (rush hour[s]) start and stop times during day
        int HTP_AM_START = 5;
        int HTP_AM_END = 10;
        int HTP_PM_START = 15;
        int HTP_PM_END = 19;
        int errors = 0;

        SimpleGroup data = value;
        StringBuilder row_str = new StringBuilder();

        // Pick and drop off column processing
        String pu_datetime_ts = getValue("tpep_pickup_datetime", data);
        row_str.append(getDateOnly(pu_datetime_ts)).append(",");
        String pu_time = getTimeOnly(pu_datetime_ts);
        row_str.append(pu_time).append(",");

        String do_datetime_ts = getValue("tpep_dropoff_datetime", data);
        row_str.append(getDateOnly(do_datetime_ts)).append(",");
        String do_time = getTimeOnly(do_datetime_ts);
        row_str.append(do_time).append(",");


        // htp_am and htp_pm
        String pu_hour = pu_time.substring(0, pu_time.indexOf(":"));
        String do_hour = do_time.substring(0, do_time.indexOf(":"));

        String htp_am = "0";
        if ((Integer.parseInt(pu_hour) > HTP_AM_START) && (Integer.parseInt(pu_hour) < HTP_AM_END) ||
                (Integer.parseInt(do_hour) > HTP_AM_START) && (Integer.parseInt(do_hour) < HTP_AM_END)) {
            htp_am = "1";
        }

        String htp_pm = "0";
        if ((Integer.parseInt(pu_hour) > HTP_PM_START) && (Integer.parseInt(pu_hour) < HTP_PM_END) ||
                (Integer.parseInt(do_hour) > HTP_PM_START) && (Integer.parseInt(do_hour) < HTP_PM_END)) {
            htp_pm = "1";
        }

        row_str.append(htp_am).append(",");
        row_str.append(htp_pm).append(",");



        // If passenger is 0, then just set to 1
        try{
            String pass_count = getValue("passenger_count", data);
            if (pass_count.equals("0") || pass_count.equals("0.0") || pass_count.equals("")){
                pass_count = "1";
            }
            row_str.append(pass_count).append(",");
        } catch(RuntimeException e){
            row_str.append("1").append(",");
        }



        // If distance is 0, then don't add the record at all (increment errors var)
        String trip_distance = getValue("trip_distance", data);
        if (trip_distance.equals("0") || trip_distance.equals("0.0") || trip_distance.equals("")){
            errors += 1;
        } else {
            row_str.append(trip_distance).append(",");
        }


        // If the pu or do locations are outside of the range of location IDs, drop the whole row
        String pu_loc_id = getValue("PULocationID", data);
        int pu_loc_id_as_int = Integer.parseInt(pu_loc_id);
        if (pu_loc_id_as_int < 1 || pu_loc_id_as_int > 263){
            errors += 1;
        } else {
            row_str.append(pu_loc_id).append(",");
        }

        String do_loc_id = getValue("DOLocationID", data);
        int do_loc_id_as_int = Integer.parseInt(do_loc_id);
        if (do_loc_id_as_int < 1 || do_loc_id_as_int > 263){
            errors += 1;
        } else {
            row_str.append(do_loc_id).append(",");
        }


        // If payment type isn't one of the types we expect (1-6), set it to 1 – Credit Card (the most common type)
        String payment_type = getValue("payment_type", data);
        if (!payment_type.equals("1") && !payment_type.equals("2") &&
                !payment_type.equals("3") && !payment_type.equals("4") &&
                !payment_type.equals("5") && !payment_type.equals("6"))
            payment_type = "1";
        row_str.append(payment_type).append(",");


        String total_amount = getValue("total_amount", data);
        if (total_amount.equals("0") || total_amount.equals("0.0") || total_amount.equals("")){
            errors += 1;
        } else {
            row_str.append(total_amount);
        }


        // Dont add any rows which have serious errors / missing data
        if (errors == 0) {
            context.write(NullWritable.get(), new Text(row_str.toString()));
        }
    }
}
