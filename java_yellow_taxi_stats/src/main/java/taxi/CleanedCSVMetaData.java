package taxi;

public class CleanedCSVMetaData {
    public static int getColIdx(ColNames col_name){
        int colIdx;
        switch (col_name) {
            case PU_DATE: colIdx = 0;
                break;
            case PU_TIME: colIdx = 1;
                break;
            case DO_DATE: colIdx = 2;
                break;
            case DO_TIME: colIdx = 3;
                break;
            case HTP_AM: colIdx = 4;
                break;
            case HTP_PM: colIdx = 5;
                break;
            case TRIP_PERIOD: colIdx = 6;
                break;
            case PASSENGER_COUNT: colIdx = 7;
                break;
            case TRIP_DISTANCE: colIdx = 8;
                break;
            case PU_LOC_ID: colIdx = 9;
                break;
            case PU_TAXI_ZONE: colIdx = 10;
                break;
            case DO_LOC_ID: colIdx = 11;
                break;
            case DO_TAXI_ZONE: colIdx = 12;
                break;
            case PAYMENT_TYPE: colIdx = 13;
                break;
            case TOTAL_COST: colIdx = 14;
                break;
            default: colIdx = -1;
                break;
        }
        return colIdx;
    }
}