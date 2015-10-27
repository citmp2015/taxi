package org.tuberlin.de.district_per_hour;


/**
 * Created by neoklosch on 27.10.15.
 */
public class Pickup {
    private String pickupDate;
    private String pickupHour;
    private double pickupLongitude;
    private double pickupLatitude;
    private int count;

    public Pickup() {
        this("", "", 0.0, 0.0, 1);
    }

    public Pickup(String pickupDate, String pickupHour, double pickupLongitude, double pickupLatitude) {
        this(pickupDate, pickupHour, pickupLongitude, pickupLatitude, 1);
    }

    public Pickup(String pickupDate, String pickupHour, double pickupLongitude, double pickupLatitude, int count) {
        this.pickupDate = pickupDate;
        this.pickupHour = pickupHour;
        this.pickupLongitude = pickupLongitude;
        this.pickupLatitude = pickupLatitude;
        this.count = count;
    }

    public String getPickupDate() {
        return pickupDate;
    }

    public void setPickupDatetime(String pickupDate) {
        this.pickupDate = pickupDate;
    }

    public double getPickupLongitude() {
        return pickupLongitude;
    }

    public void setPickupLongitude(double pickupLongitude) {
        this.pickupLongitude = pickupLongitude;
    }

    public double getPickupLatitude() {
        return pickupLatitude;
    }

    public void setPickupLatitude(double pickupLatitude) {
        this.pickupLatitude = pickupLatitude;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setPickupDate(String pickupDate) {
        this.pickupDate = pickupDate;
    }

    public String getPickupHour() {
        return pickupHour;
    }

    public void setPickupHour(String pickupHour) {
        this.pickupHour = pickupHour;
    }

    @Override
    public String toString() {
        return "Pickup{" +
                "pickupDatetime=" + pickupDate +
                ", pickupHour=" + pickupHour +
                ", pickupLongitude=" + pickupLongitude +
                ", pickupLatitude=" + pickupLatitude +
                ", count=" + count +
                '}';
    }
}
