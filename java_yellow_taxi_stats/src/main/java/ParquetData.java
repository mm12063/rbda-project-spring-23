public class ParquetData {
    public static int getColIdx(ColNames col_name){
        return switch (col_name) {
            case PU_DATE -> 0;
            case PU_TIME -> 1;
            case DO_DATE -> 2;
            case DO_TIME -> 3;
            case HTP_AM -> 4;
            case HTP_PM -> 5;
            case PASSENGER_COUNT -> 6;
            case TRIP_DISTANCE -> 7;
            case PU_LOC_ID -> 8;
            case DO_LOC_ID -> 9;
            case PAYMENT_TYPE -> 10;
            case TOTAL_COST -> 11;
        };
    }
}
