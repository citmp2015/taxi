package org.tuberlin.de.read_data;

import org.opengis.geometry.primitive.Point;

import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Fabian on 20.10.2015.
 * modified by Gerrit on 22.10.2015
 *
 * Taxidrive is a POJO-Type. Conditions:
 *
 * The class is public and standalone (no non-static inner class)
 * The class has a public no-argument constructor
 * All fields in the class (and all superclasses) are either public or or have a public getter and a setter method that follows the Java beans naming conventions for getters and setters.
 * (https://ci.apache.org/projects/flink/flink-docs-master/internals/types_serialization.html)
 *
 */

public class Taxidrive {
  public String taxiID;
  public String licenseID;
  public String pickup_datetime;
  public String dropoff_datetime;
  public int trip_time_in_secs;
  public double trip_distance;
  public double pickup_longitude;
  public double pickup_latitude;
  public double dropoff_longitude;
  public double dropoff_latitude;
  public String payment_type;
  public double fare_amount;
  public double surcharge;
  public double mta_tax;
  public double tip_amount;
  public double tolls_amount;
  public double total_amount;
  public String pickupDistrict;
  public String dropoffDistrict;


  //DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public Taxidrive(){}

  public String getTaxiID() {
    return taxiID;
  }

  public void setTaxiID(String taxiID) {
    this.taxiID = taxiID;
  }

  public String getLicenseID() {
    return licenseID;
  }

  public void setLicenseID(String licenseID) {
    this.licenseID = licenseID;
  }

  public String getPickup_datetime() {
    /*try {
      return dateFormat.parse(pickup_datetime);
    } catch (ParseException e) {
      e.printStackTrace();
    }*/
    return pickup_datetime;
  }

  public void setPickup_datetime(String pickup_datetime) {
    this.pickup_datetime = pickup_datetime;
  }

  public String getDropoff_datetime() {
    return dropoff_datetime;
  }

  public void setDropoff_datetime(String dropoff_datetime) {
    this.dropoff_datetime = dropoff_datetime;
  }

  public int getTrip_time_in_secs() {
    return trip_time_in_secs;
  }

  public void setTrip_time_in_secs(int trip_time_in_secs) {
    this.trip_time_in_secs = trip_time_in_secs;
  }

  public double getTrip_distance() {
    return trip_distance;
  }

  public void setTrip_distance(double trip_distance) {
    this.trip_distance = trip_distance;
  }

  public double getPickup_longitude() {
    return pickup_longitude;
  }

  public void setPickup_longitude(double pickup_longitude) {
    this.pickup_longitude = pickup_longitude;
  }

  public double getPickup_latitude() {
    return pickup_latitude;
  }

  public void setPickup_latitude(double pickup_latitude) {
    this.pickup_latitude = pickup_latitude;
  }

  public double getDropoff_longitude() {
    return dropoff_longitude;
  }

  public void setDropoff_longitude(double dropoff_longitude) {
    this.dropoff_longitude = dropoff_longitude;
  }

  public double getDropoff_latitude() {
    return dropoff_latitude;
  }

  public String getPickupDistrict(){ return pickupDistrict; }
  public String getDropoffDistrict() {return dropoffDistrict;}

  public void setDropoffDistrict(String dropoffDistrict) {this.dropoffDistrict = dropoffDistrict; }
  public void setPickupDistrict(String pickupDistrict) {this.pickupDistrict = pickupDistrict; }



  public void setDropoff_latitude(double dropoff_latitude) {
    this.dropoff_latitude = dropoff_latitude;
  }

  public String getPayment_type() {
    return payment_type;
  }

  public void setPayment_type(
                  String payment_type) {
    this.payment_type = payment_type;
  }

  public double getFare_amount() {
    return fare_amount;
  }

  public void setFare_amount(double fare_amount) {
    this.fare_amount = fare_amount;
  }

  public double getSurcharge() {
    return surcharge;
  }

  public void setSurcharge(double surcharge) {
    this.surcharge = surcharge;
  }

  public double getMta_tax() {
    return mta_tax;
  }

  public void setMta_tax(double mta_tax) {
    this.mta_tax = mta_tax;
  }

  public double getTip_amount() {
    return tip_amount;
  }

  public void setTip_amount(double tip_amount) {
    this.tip_amount = tip_amount;
  }

  public double getTolls_amount() {
    return tolls_amount;
  }

  public void setTolls_amount(double tolls_amount) {
    this.tolls_amount = tolls_amount;
  }

  public double getTotal_amount() {
    return total_amount;
  }

  public void setTotal_amount(double total_amount) {
    this.total_amount = total_amount;
  }

  @Override
  public String toString() {
    return "Taxidrive{" +
            "taxiID='" + taxiID + '\'' +
            ", licenseID='" + licenseID + '\'' +
            ", pickup_datetime='" + pickup_datetime + '\'' +
            ", dropoff_datetime='" + dropoff_datetime + '\'' +
            ", trip_time_in_secs=" + trip_time_in_secs +
            ", trip_distance=" + trip_distance +
            ", pickup_longitude=" + pickup_longitude +
            ", pickup_latitude=" + pickup_latitude +
            ", dropoff_longitude=" + dropoff_longitude +
            ", dropoff_latitude=" + dropoff_latitude +
            ", payment_type='" + payment_type + '\'' +
            ", fare_amount=" + fare_amount +
            ", surcharge=" + surcharge +
            ", mta_tax=" + mta_tax +
            ", tip_amount=" + tip_amount +
            ", tolls_amount=" + tolls_amount +
            ", total_amount=" + total_amount +
            ", pickupDistrict='" + pickupDistrict + '\'' +
            ", dropoffDistrict='" + dropoffDistrict + '\'' +
            '}';
  }

}
