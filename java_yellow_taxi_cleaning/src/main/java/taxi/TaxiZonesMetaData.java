package taxi;

public class TaxiZonesMetaData {
    public static int getColIdx(ColNames col_name) {
        int colIdx;
        switch (col_name) {
            case VENDOR_ID: colIdx = 0;
                break;
            case TPEP_PICKUP_DATETIME: colIdx = 1;
                break;
            case TPEP_DROPOFF_DATETIME: colIdx = 2;
                break;
            case PASSENGER_COUNT: colIdx = 3;
                break;
            case TRIP_DISTANCE: colIdx = 4;
                break;
            case RATECODE_ID: colIdx = 5;
                break;
            case STORE_AND_FWD_FLAG: colIdx = 6;
                break;
            case PU_LOCATION_ID: colIdx = 7;
                break;
            case DO_LOCATION_ID: colIdx = 8;
                break;
            case PAYMENT_TYPE: colIdx = 9;
                break;
            case FARE_AMOUNT: colIdx = 10;
                break;
            case EXTRA: colIdx = 11;
                break;
            case MTA_TAX: colIdx = 12;
                break;
            case TIP_AMOUNT: colIdx = 13;
                break;
            case TOLLS_AMOUNT: colIdx = 14;
                break;
            case IMPROVEMENT_SURCHARGE: colIdx = 15;
                break;
            case TOTAL_AMOUNT: colIdx = 16;
                break;
            case CONGESTION_CHARGE: colIdx = 17;
                break;
            case AIRPORT_FEE: colIdx = 18;
                break;
            default: colIdx = -1;
                break;
        }
        return colIdx;
    }
}
