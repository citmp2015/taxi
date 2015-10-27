package org.tuberlin.de.district_per_hour;


public class Pickup {
    private String pickupDate;
    private String pickupHour;
    private String district;
    private int count;

    private Pickup(Builder builder) {

    }

    public static class Builder {
        private String pickupDate;
        private String pickupHour;
        private String district;
        private double pickupLongitude;
        private double pickupLatitude;
        private int count;

        public Builder() {
            count = 1;
        }

        public Builder setPickupDate(String pickupDate) {
            this.pickupDate = pickupDate;
            return this;
        }

        public Builder setPickupHour(String pickupHour) {
            this.pickupHour = pickupHour;
            return this;
        }

        public Builder setDistrict(String district) {
            this.district = district;
            return this;
        }

        public Builder setCount(int count) {
            this.count = count;
            return this;
        }

        public Pickup build() {
            return new Pickup(this);
        }
    }

    public String getPickupDate() {
        return pickupDate;
    }

    public int getCount() {
        return count;
    }

    public String getPickupHour() {
        return pickupHour;
    }

    public String getDistrict() {
        return district;
    }

    @Override
    public String toString() {
        return "Pickup{" +
                "pickupDatetime=" + pickupDate +
                ", pickupHour=" + pickupHour +
                ", district=" + district +
                ", count=" + count +
                '}';
    }
}
