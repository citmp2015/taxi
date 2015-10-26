package org.tuberlin.de.best_taxidriver;

import org.apache.flink.api.java.tuple.Tuple11;

/**
 * Created by gerrit on 26.10.15.
 */
public class DriverStat {
    public String licenseID;
    public int tripTimeSum_inSecs;
    public double trip_distanceSum;
    public double tip_amountSum;
    public double tolls_amountSum;
    public double total_amountSum;
    public int countOfTrips;

    public DriverStat() {
    }


    public double getAvgTripTime(){
        return tripTimeSum_inSecs/ countOfTrips;
    }

    public double getAvgTollAmount(){
        return tolls_amountSum/ countOfTrips;
    }

    public double getAvgTotalAmount(){
        return total_amountSum/ countOfTrips;
    }

    public double getAvgTripDistance(){
        return trip_distanceSum/ countOfTrips;
    }

    public Tuple11<String, Integer, Double, Double, Double, Double, Integer, Double, Double, Double, Double> getStatAsTuple(){
        return new Tuple11<>(licenseID,
                tripTimeSum_inSecs,
                trip_distanceSum,
                tip_amountSum,
                tolls_amountSum,
                total_amountSum,
                countOfTrips,
                getAvgTripTime(),
                getAvgTollAmount(),
                getAvgTotalAmount(),
                getAvgTripDistance()
        );
    }

    public static String getCSVHeader(){
        return "licenseID," +
                "tripTimeSum_inSecs," +
                "trip_distanceSum," +
                "tip_amountSum," +
                "tolls_amountSum," +
                "total_amountSum,"+
                "countOfTrips," +
                "avgTripTime," +
                "avgTollAmount," +
                "avgTotalAmount," +
                "avgTripDistance\n";
    }

    @Override
    public String toString() {
        return "DriverStat{" +
                "licenseID='" + licenseID + '\'' +
                ", tripTimeSum_inSecs=" + tripTimeSum_inSecs +
                ", trip_distanceSum=" + trip_distanceSum +
                ", tip_amountSum=" + tip_amountSum +
                ", tolls_amountSum=" + tolls_amountSum +
                ", total_amountSum=" + total_amountSum +
                ", countOfTrips=" + countOfTrips +
                ", avgtripTime="+getAvgTripTime()+
                ", avgTripDistance="+getAvgTripDistance()+
                ", avgTollAmount" + getAvgTollAmount()+
                ", avgTotalAmount="+getAvgTotalAmount()+
                '}';
    }


}
