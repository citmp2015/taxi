package org.tuberlin.de.clock_average_speed;

public class TaxiTrip {
	private String time;
	private int count;
	private double speed;
	private double averageSpeed;

	public TaxiTrip() {
		this.count = 1;
		this.speed = 0.0;
		this.averageSpeed = 0.0;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public double getAverageSpeed() {
		return averageSpeed;
	}

	public void setAverageSpeed(double averageSpeed) {
		this.averageSpeed = averageSpeed;
	}

	@Override
	public String toString() {
		return "TaxiTrip [time=" + time + ", count=" + count + ", speed=" + speed + ", averageSpeed=" + averageSpeed
				+ "]";
	}

}
