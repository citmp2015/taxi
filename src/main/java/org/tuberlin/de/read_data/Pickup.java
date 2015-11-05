package org.tuberlin.de.read_data;


public class Pickup {
    private String date;
    private String hour;
    private String district;
    private int count;

    /**
     * For some reason it is necessary to have a default constructor.
     */
    public Pickup() {
        this.date = "2015-10-25";
        this.hour = "12";
        this.district = "NYC";
        this.count = 1;
    }

    public Pickup(Builder builder) {
        this.date = builder.date;
        this.hour = builder.hour;
        this.district = builder.district;
        this.count = builder.count;
    }

    public static class Builder {
        private String date;
        private String hour;
        private String district;
        private int count;

        public Builder() {
            count = 1;
        }

        public Builder setDate(String date) {
            this.date = date;
            return this;
        }

        public Builder setHour(String hour) {
            this.hour = hour;
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

    public String getDate() {
        return date;
    }

    public String getHour() {
        return hour;
    }

    public String getDistrict() {
        return district;
    }

    public int getCount() {
        return count;
    }

    /**
     * For some reason it is necessary to have a setter for every attribute.
     */
    public void setDate(String date) {
        this.date = date;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Pickup{" +
                "date=" + date +
                ", hour=" + hour +
                ", neighborhood=" + district +
                ", count=" + count +
                '}';
    }
}
