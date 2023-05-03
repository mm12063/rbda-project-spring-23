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
import java.util.logging.Logger;

public class YellowTaxiMapper extends Mapper<LongWritable, SimpleGroup, NullWritable, Text> {
    private final HashMap<String, String> taxi_zones = new HashMap<>();
    private final HashMap<String, String> payment_types = new HashMap<>();
    private final Logger log = Logger.getLogger(YellowTaxiMapper.class.getName());

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
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] data = line.split(",");
                    if (!data[0].equals("OBJECTID")) {
                        taxi_zones.put(data[4], data[3] + "," + data[5]);
                    }
                }
                reader.close();

            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
            }
        }


        // Payment types
        payment_types.put("1", "Credit card");
        payment_types.put("2", "Cash");
        payment_types.put("3", "No charge");
        payment_types.put("4", "Dispute");
        payment_types.put("5", "Unknown");
        payment_types.put("6", "Voided trip");

        String csv_header =
                "pu_month" + "," +
                "pu_day" + "," +
                "pu_year" + "," +
                "pu_time" + "," +
                "do_month" + "," +
                "do_day" + "," +
                "do_year" + "," +
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
    }

    private String getValue(int col_idx, SimpleGroup value) {
        return value.getValueToString(col_idx, 0);
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

    private String splitDate(String full_date) {
        StringBuilder row_str = new StringBuilder();
        String[] date_parts = full_date.split("-");
        row_str.append(date_parts[0]).append(","); // Month
        row_str.append(date_parts[1]).append(","); // Day
        row_str.append(date_parts[2]).append(","); // Year
        return row_str.toString();
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

        StringBuilder row_str = new StringBuilder();

        // Pick up date time
        String pu_datetime_ts = getValue(TaxiZonesMetaData.getColIdx(ColNames.TPEP_PICKUP_DATETIME), value);
        String pu_full_date = getDateOnly(pu_datetime_ts);
        row_str.append(splitDate(pu_full_date));
        String pu_time = getTimeOnly(pu_datetime_ts);
        row_str.append(pu_time).append(",");

        // Drop off date time
        String do_datetime_ts = getValue(TaxiZonesMetaData.getColIdx(ColNames.TPEP_DROPOFF_DATETIME), value);
        String do_full_date = getDateOnly(do_datetime_ts);
        row_str.append(splitDate(do_full_date));
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
        row_str.append(htp_am).append(",");

        String htp_pm = "0";
        if ((Integer.parseInt(pu_hour) > HTP_PM_START) && (Integer.parseInt(pu_hour) < HTP_PM_END) ||
                (Integer.parseInt(do_hour) > HTP_PM_START) && (Integer.parseInt(do_hour) < HTP_PM_END)) {
            htp_pm = "1";
        }
        row_str.append(htp_pm).append(",");



        // Add period of day for the trip  Morning/Afternoon/Evening/Late Evening/Late Night
        row_str.append(get_day_period(pu_time)).append(",");



        // If passenger is 0, then just set to 1
        try {
            String pass_count = getValue(TaxiZonesMetaData.getColIdx(ColNames.PASSENGER_COUNT), value);
            if (pass_count.equals("0") || pass_count.equals("0.0") || pass_count.equals(""))
                pass_count = "1";
            int pass_count_int = (int)Double.parseDouble(pass_count);
            row_str.append(pass_count_int).append(",");
        } catch (RuntimeException e) {
            row_str.append("1").append(",");
        }


        // If distance is 0, then don't add the record at all (increment errors var)
        try {
            String trip_distance = getValue(TaxiZonesMetaData.getColIdx(ColNames.TRIP_DISTANCE), value);
            if (trip_distance.equals("0") || trip_distance.equals("0.0") || trip_distance.equals("")) {
                errors += 1;
            } else {
                row_str.append(trip_distance).append(",");
            }
        } catch (RuntimeException e) {
            System.out.println("Exception: trip_distance");
            log.info(e.getMessage());
        }


        // If the pu or do locations are outside of the range of location IDs, drop the whole row
        String pu_loc_id = getValue(TaxiZonesMetaData.getColIdx(ColNames.PU_LOCATION_ID), value);
        if (errors == 0) {
            int pu_loc_id_as_int = Integer.parseInt(pu_loc_id);
            if (pu_loc_id_as_int < 1 || pu_loc_id_as_int > 263) {
                errors += 1;
            } else {
                row_str.append(pu_loc_id).append(",");
            }
        }

        try {
            if (errors == 0) {
                String[] area_borough = taxi_zones.get(pu_loc_id).split(",");
                String area = area_borough[0];
                String borough = area_borough[1];

                if (!borough.equals("Manhattan")) {
                    errors += 1;
                } else {
                    row_str.append(area).append(",");
                }
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            log.info(pu_loc_id +" "+ taxi_zones.get(pu_loc_id));
        }


        String do_loc_id = getValue(TaxiZonesMetaData.getColIdx(ColNames.DO_LOCATION_ID), value);
        try {
            if (errors == 0) {
                int do_loc_id_as_int = Integer.parseInt(do_loc_id);
                if (do_loc_id_as_int < 1 || do_loc_id_as_int > 263) {
                    errors += 1;
                } else {
                    row_str.append(do_loc_id).append(",");
                }
            }

            if (errors == 0) {
                String[] area_borough = taxi_zones.get(do_loc_id).split(",");
                String area = area_borough[0];
                row_str.append(area).append(",");
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            log.info(do_loc_id +" "+ taxi_zones.get(do_loc_id));
        }



        // If payment type isn't one of the types we expect (1-6), set it to 1  Credit Card (the most common type)
        String payment_type = getValue(TaxiZonesMetaData.getColIdx(ColNames.PAYMENT_TYPE), value);
        if (errors == 0) {
            if (payment_types.containsKey(payment_type))
                row_str.append(payment_types.get(payment_type)).append(",");
            else
                // Set default as "1" Credit card
                row_str.append(payment_types.get("1")).append(",");
        }


        if (errors == 0) {
            String total_amount = getValue(TaxiZonesMetaData.getColIdx(ColNames.TOTAL_AMOUNT), value);
            if (total_amount.equals("0") || total_amount.equals("0.0") || total_amount.equals("")) {
                errors += 1;
            } else {
                row_str.append(total_amount);
            }
        }


        // Dont add any rows which have serious errors / missing data
        if (errors == 0)
            context.write(NullWritable.get(), new Text(row_str.toString()));
    }
}
