package org.tuberlin.de.distance_tip;

public class TipAndDistance {
    public double tip;
    public double distance;
    public int roundedDistance; // rounded down, used as groupkey
    public int tripAmount;

    public TipAndDistance() {
    }

    public TipAndDistance(double tip, double distance) {
        this(tip, distance, 1, (int) distance);
    }

    // Only for Aggregation
    public TipAndDistance(double tip, double distance, int tripAmount, int groupKey) {
        this.tip = tip;
        this.distance = distance;
        this.tripAmount = tripAmount;
        roundedDistance = groupKey;
    }

    @Override
    public String toString() {
        // GroupKey (rounded distance in miles), average tip per mile, sum of tips, sum of distance, amount of trips, avg tip per trip (everything per group)
        return roundedDistance + ", " + (tip/distance) + ", " + tip + ", " + distance +  ", " + tripAmount + ", " + (tip/tripAmount);
    }
}