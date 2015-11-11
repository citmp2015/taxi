package org.tuberlin.de.district_tip;

public class TaxiTip {
	private String district;
	private int count;
	private double tip;
	private double averageTip;

	public TaxiTip() {
		this.count = 1;
		this.tip = 0.0;
		this.averageTip = 0.0;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getTip() {
		return tip;
	}

	public void setTip(double tip) {
		this.tip = tip;
	}

	public double getAverageTip() {
		return averageTip;
	}

	public void setAverageTip(double averageTip) {
		this.averageTip = averageTip;
	}

	@Override
	public String toString() {
		return "TaxiTrip [district=" + district + ", count=" + count + ", tip=" + tip + ", averageTip=" + averageTip
				+ "]";
	}

}
