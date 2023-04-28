public class TaxiZonesMetaData {
    public static int getColIdx(ColNames col_name){
        return switch (col_name) {
            case VENDOR_ID -> 0;
            case TPEP_PICKUP_DATETIME -> 1;
            case TPEP_DROPOFF_DATETIME -> 2;
            case PASSENGER_COUNT -> 3;
            case TRIP_DISTANCE -> 4;
            case RATECODE_ID -> 5;
            case STORE_AND_FWD_FLAG -> 6;
            case PU_LOCATION_ID -> 7;
            case DO_LOCATION_ID -> 8;
            case PAYMENT_TYPE -> 9;
            case FARE_AMOUNT -> 10;
            case EXTRA -> 11;
            case MTA_TAX -> 12;
            case TIP_AMOUNT -> 13;
            case TOLLS_AMOUNT -> 14;
            case IMPROVEMENT_SURCHARGE -> 15;
            case TOTAL_AMOUNT -> 16;
            case CONGESTION_CHARGE -> 17;
            case AIRPORT_FEE -> 18;
        };
    }
}