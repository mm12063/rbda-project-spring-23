package taxi;

import java.io.*;
import java.time.LocalTime;
import java.util.*;
import java.util.Date;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;

public class YellowTaxiMapper extends Mapper<LongWritable, SimpleGroup, NullWritable, Text> {
    private final HashMap<String, String> taxi_zones = new HashMap<>();
    private final HashMap<String, String> payment_types = new HashMap<>();

    private String get_day_period(String hours_mins) {
        LocalTime pu_time = LocalTime.parse(hours_mins);
        LocalTime morn_st = LocalTime.parse("06:00");
        LocalTime morn_end = LocalTime.parse("12:00");
        LocalTime eve_st = LocalTime.parse("18:00");
        LocalTime eve_end = LocalTime.parse("21:00");
        LocalTime late_eve_end = LocalTime.parse("23:59");

        if (pu_time.isAfter(morn_st) && pu_time.isBefore(morn_end)) {
            return "Morning";
        } else if (pu_time.isAfter(morn_end) && pu_time.isBefore(eve_st)) {
            return "Afternoon";
        } else if (pu_time.isAfter(eve_st) && pu_time.isBefore(eve_end)) {
            return "Evening";
        } else if (pu_time.isAfter(eve_end) && pu_time.isBefore(late_eve_end)) {
            return "Late Evening";
        }
        return "Late Night";
    }


    public void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Set up running");

//        URI[] cacheFiles = context.getCacheFiles();
//        if (cacheFiles != null && cacheFiles.length > 0) {
//
//            try {
//                FileSystem fs = FileSystem.get(context.getConfiguration());
//                Path getFilePath = new Path(cacheFiles[0].toString());
//
////                String service_id_file_location = context.getConfiguration().get("taxi.zones.file.path");
//                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
//
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    String[] data = line.split(",");
//                    if (!data[0].equals("OBJECTID")) {
//                        taxi_zones.put(data[4], data[3] + "," + data[5]);
//                    }
//                }
//                System.out.println("Loaded in!");
//                reader.close();
//
////        for (Map.Entry<String, String> t : taxi_zones.entrySet()) {
////            System.out.println(t.getKey() + " " + t.getValue());
////        }
//
//            } catch (Exception ex) {
//                System.out.println(ex.getLocalizedMessage());
//            }
//        }


//        assertEquals(get_day_period("04:23"), "Late Night");
//        assertEquals(get_day_period("09:23"), "Morning");
//        assertEquals(get_day_period("12:02"), "Afternoon");
//        assertEquals(get_day_period("18:02"), "Evening");
//        assertEquals(get_day_period("21:02"), "Late Evening");
//        assertEquals(get_day_period("00:02"), "Late Night");


//        taxi_zones.put("1", "test");
//        taxi_zones.put("2", "test");
//        taxi_zones.put("3", "test");
//        taxi_zones.put("4", "test");


        // Payment types
        payment_types.put("1", "Credit card");
        payment_types.put("2", "Cash");
        payment_types.put("3", "No charge");
        payment_types.put("4", "Dispute");
        payment_types.put("5", "Unknown");
        payment_types.put("6", "Voided trip");


        String csv_header = "pu_date" + "," +
                "pu_time" + "," +
                "do_date" + "," +
                "do_time" + "," +
                "htp_am" + "," +
                "htp_pm" + "," +
                "trip_period" + "," +
                "passenger_count" + "," +
                "trip_distance" + "," +
                "pu_loc_id" + "," +
                "pu_taxi_zone" + "," +
                "do_loc_id" + "," +
                "do_taxi_zone" + "," +
                "payment_type_readable" + "," +
                "total_cost";

        context.write(NullWritable.get(), new Text(csv_header));

        System.out.println("Set up END");
    }

    private String getValue(int col_idx, SimpleGroup value) {
        return value.getValueToString(col_idx, 0);
    }

    private String getValue(int col_idx, String[] value) {
        return value[col_idx + 1];
    }

    private String getDateOnly(String timestamp) {
        long ts = Long.parseLong(timestamp) / 1000;
        Date date_only = new Date(ts);
        DateFormat f = new SimpleDateFormat("MM-dd-yyyy", Locale.ENGLISH);
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f.format(date_only);
    }

    private String getTimeOnly(String timestamp) {
        long ts = Long.parseLong(timestamp) / 1000;
        Date date_only = new Date(ts);
        DateFormat f = new SimpleDateFormat("HH:mm", Locale.ENGLISH);
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f.format(date_only);
    }

    @Override
    public void map(LongWritable key, SimpleGroup value, Context context)
            throws IOException, InterruptedException {
        System.out.println("map start");

        // High Traffic Period (rush hour[s]) start and stop times during day
        int HTP_AM_START = 5;
        int HTP_AM_END = 10;
        int HTP_PM_START = 15;
        int HTP_PM_END = 19;
        int errors = 0;

        SimpleGroup data = value;
//        Text data = value;
//        String[] data = value.toString().split(",");
        StringBuilder row_str = new StringBuilder();

//        if (!data[1].toString().equals("VendorID")) {
//
//        // Pick and drop off column processing
//        String pu_datetime_ts = getValue(TaxiZonesMetaData.getColIdx(ColNames.TPEP_PICKUP_DATETIME), data);
////            String pu_datetime_ts = "1682054966000";
//        row_str.append(getDateOnly(pu_datetime_ts)).append(",");
//        String pu_time = getTimeOnly(pu_datetime_ts);
//        row_str.append(pu_time).append(",");
//
//
//        String do_datetime_ts = getValue(TaxiZonesMetaData.getColIdx(ColNames.TPEP_DROPOFF_DATETIME), data);
////            String do_datetime_ts = "1682054966000";
//        row_str.append(getDateOnly(do_datetime_ts)).append(",");
//        String do_time = getTimeOnly(do_datetime_ts);
//        row_str.append(do_time).append(",");
//
//
//        // htp_am and htp_pm
//        String pu_hour = pu_time.substring(0, pu_time.indexOf(":"));
//        String do_hour = do_time.substring(0, do_time.indexOf(":"));
//
//        String htp_am = "0";
//        if ((Integer.parseInt(pu_hour) > HTP_AM_START) && (Integer.parseInt(pu_hour) < HTP_AM_END) ||
//                (Integer.parseInt(do_hour) > HTP_AM_START) && (Integer.parseInt(do_hour) < HTP_AM_END)) {
//            htp_am = "1";
//        }
//        row_str.append(htp_am).append(",");
//
//        String htp_pm = "0";
//        if ((Integer.parseInt(pu_hour) > HTP_PM_START) && (Integer.parseInt(pu_hour) < HTP_PM_END) ||
//                (Integer.parseInt(do_hour) > HTP_PM_START) && (Integer.parseInt(do_hour) < HTP_PM_END)) {
//            htp_pm = "1";
//        }
//        row_str.append(htp_pm).append(",");
//
//
//        System.out.println("2");
//
//        // Add period of day for the trip  Morning/Afternoon/Evening/Late Evening/Late Night
//        row_str.append(get_day_period(pu_time)).append(",");
//
//
//        // If passenger is 0, then just set to 1
//        try {
//            String pass_count = getValue(TaxiZonesMetaData.getColIdx(ColNames.PASSENGER_COUNT), data);
//            if (pass_count.equals("0") || pass_count.equals("0.0") || pass_count.equals(""))
//                pass_count = "1";
//            row_str.append(pass_count).append(",");
//        } catch (RuntimeException e) {
//            row_str.append("1").append(",");
//        }
//
//
//        // If distance is 0, then don't add the record at all (increment errors var)
//        String trip_distance = getValue(TaxiZonesMetaData.getColIdx(ColNames.TRIP_DISTANCE), data);
//        if (trip_distance.equals("0") || trip_distance.equals("0.0") || trip_distance.equals(""))
//            errors += 1;
//        else
//            row_str.append(trip_distance).append(",");
//
//
//        // If the pu or do locations are outside of the range of location IDs, drop the whole row
//        String pu_loc_id = getValue(TaxiZonesMetaData.getColIdx(ColNames.PU_LOCATION_ID), data);
//        if (errors == 0) {
//            int pu_loc_id_as_int = Integer.parseInt(pu_loc_id);
//            if (pu_loc_id_as_int < 1 || pu_loc_id_as_int > 263)
//                errors += 1;
//            else
//                row_str.append(pu_loc_id).append(",");
//        }
//
//        if (errors == 0) {
//            System.out.println(pu_loc_id);
////            String[] area_borough = taxi_zones.get("1").split(",");
////            String area = area_borough[0];
////            String borough = area_borough[1];
//
////            if (!borough.equals("Manhattan"))
////                errors += 1;
////            else
////                row_str.append(area).append(",");
//            row_str.append("HELLO").append(",");
//        }
//
//
//        String do_loc_id = getValue(TaxiZonesMetaData.getColIdx(ColNames.DO_LOCATION_ID), data);
//        if (errors == 0) {
//            int do_loc_id_as_int = Integer.parseInt(do_loc_id);
//            if (do_loc_id_as_int < 1 || do_loc_id_as_int > 263)
//                errors += 1;
//            else
//                row_str.append(do_loc_id).append(",");
//        }
//
//        if (errors == 0) {
////            String[] area_borough = taxi_zones.get(do_loc_id).split(",");
////            String area = area_borough[0];
////            row_str.append(area).append(",");
//            row_str.append("HOWDY").append(",");
//        }
//
//
//        // If payment type isn't one of the types we expect (1-6), set it to 1  Credit Card (the most common type)
//        String payment_type = getValue(TaxiZonesMetaData.getColIdx(ColNames.PAYMENT_TYPE), data);
//        if (errors == 0) {
//            if (payment_types.containsKey(payment_type))
//                row_str.append(payment_types.get(payment_type)).append(",");
//            else
//                // Set default as "1" Credit card
//                row_str.append(payment_types.get("1")).append(",");
//        }
//
//
//        if (errors == 0) {
//            String total_amount = getValue(TaxiZonesMetaData.getColIdx(ColNames.TOTAL_AMOUNT), data);
//            if (total_amount.equals("0") || total_amount.equals("0.0") || total_amount.equals(""))
//                errors += 1;
//            else
//                row_str.append(total_amount);
//        }


        // Dont add any rows which have serious errors / missing data
//        if (errors == 0)
//            context.write(NullWritable.get(), new Text(row_str.toString()));
        context.write(NullWritable.get(), new Text("Running"));

//        }
    }
}
