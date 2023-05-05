package taxi;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;

public class YellowTaxiStatsTuple implements Writable {

    private double min = 0.0;
    private double max = 0.0;
    private double dist_avg = 0.0;
    private double pass_avg = 0;
    private double cost_avg = 0;
    private long count = 0;

    public void setMin(double min) {this.min = min;}
    public double getMin() {return min;}
    public void setMax(double max) {this.max = max;}
    public double getMax() {return max;}
    public void setDistAvg(double dist_avg) {this.dist_avg = Math.round(dist_avg * 100.0) / 100.0;;}
    public double getDistAvg() {return dist_avg;}
    public void setPassAvg(double pass_avg) {this.pass_avg = Math.round(pass_avg * 100.0) / 100.0;}
    public double getPassAvg() {return pass_avg;}
    public void setCostAvg(double cost_avg) {this.cost_avg = Math.round(cost_avg * 100.0) / 100.0;}
    public double getCostAvg() {return cost_avg;}
    public void setCount(long count) {this.count = count;}
    public long getCount() {return count;}

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
        dist_avg = in.readDouble();
        pass_avg = in.readDouble();
        cost_avg = in.readDouble();
        count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(dist_avg);
        out.writeDouble(pass_avg);
        out.writeDouble(cost_avg);
        out.writeLong(count);
    }
}
