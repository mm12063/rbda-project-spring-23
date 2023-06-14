package taxi;

public class CleanedCSVMetaData {
    public static int getColIdx(ColNames col_name){
        int colIdx;
        switch (col_name) {
            case PU_MONTH: colIdx = 0;
                break;
            case PU_DAY: colIdx = 1;
                break;
            case PU_YEAR: colIdx = 2;
                break;
            case PU_TIME: colIdx = 3;
                break;
            case DO_MONTH: colIdx = 4;
                break;
            case DO_DAY: colIdx = 5;
                break;
            case DO_YEAR: colIdx = 6;
                break;
            case DO_TIME: colIdx = 7;
                break;
            case HTP_AM: colIdx = 8;
                break;
            case HTP_PM: colIdx = 9;
                break;
            case TRIP_PERIOD: colIdx = 10;
                break;
            case PASSENGER_COUNT: colIdx = 11;
                break;
            case TRIP_DISTANCE: colIdx = 12;
                break;
            case PU_LOC_ID: colIdx = 13;
                break;
            case PU_TAXI_ZONE: colIdx = 14;
                break;
            case DO_LOC_ID: colIdx = 15;
                break;
            case DO_TAXI_ZONE: colIdx = 16;
                break;
            case PAYMENT_TYPE: colIdx = 17;
                break;
            case TOTAL_COST: colIdx = 18;
                break;
            default: colIdx = -1;
                break;
        }
        return colIdx;
    }
}
