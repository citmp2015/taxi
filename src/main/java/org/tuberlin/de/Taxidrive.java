package org.tuberlin.de;

import org.opengis.geometry.primitive.Point;

import java.math.BigInteger;
import java.util.Date;

/**
 * Created by Fabian on 20.10.2015.
 */
public class Taxidrive {
  private BigInteger taxiID;
  private BigInteger licenseID;
  private Date pickup_datetime;
  private Date dropoff_datetime;
  private int trip_time_in_secs;
  private double trip_distance;
  private double pickup_longitude;
  private double pickup_latitude;
  private double dropoff_longitude;
  private double dropoff_latitude;
  private PAYMENT_TYPE payment_type;
  private double fare_amount;
  private double surcharge;
  private double mta_tax;
  private double tip_amount;
  private double tolls_amount;
  private double total_amount;

  public Taxidrive(){

  }

  public BigInteger getTaxiID() {
    return taxiID;
  }

  public void setTaxiID(BigInteger taxiID) {
    this.taxiID = taxiID;
  }

  public BigInteger getLicenseID() {
    return licenseID;
  }

  public void setLicenseID(BigInteger licenseID) {
    this.licenseID = licenseID;
  }

  public Date getPickup_datetime() {
    return pickup_datetime;
  }

  public void setPickup_datetime(Date pickup_datetime) {
    this.pickup_datetime = pickup_datetime;
  }

  public Date getDropoff_datetime() {
    return dropoff_datetime;
  }

  public void setDropoff_datetime(Date dropoff_datetime) {
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

  public void setDropoff_latitude(double dropoff_latitude) {
    this.dropoff_latitude = dropoff_latitude;
  }

  public PAYMENT_TYPE getPayment_type() {
    return payment_type;
  }

  public void setPayment_type(
                  PAYMENT_TYPE payment_type) {
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

  public enum PAYMENT_TYPE {
    CASH, CREDIT_CARD
  }
}
