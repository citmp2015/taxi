package org.tuberlin.de.read_data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Job {
    public static final class TaxidriveReader implements FlatMapFunction<String, Taxidrive> {
        @Override
        public void flatMap(String value, Collector<Taxidrive> out) throws Exception {
            Taxidrive taxidrive = new Taxidrive();

            String[] splittedText = value.split(",");
            taxidrive.setTaxiID(splittedText[0]);
            taxidrive.setLicenseID(splittedText[1]);
            taxidrive.setPickup_datetime(splittedText[2]);
            taxidrive.setDropoff_datetime(splittedText[3]);
            taxidrive.setTrip_time_in_secs(Integer.parseInt(splittedText[4]));
            taxidrive.setTrip_distance(Double.parseDouble(splittedText[5]));
            if (!splittedText[6].equals("")) taxidrive.setPickup_longitude(Double.parseDouble(splittedText[6]));
            if (!splittedText[7].equals("")) taxidrive.setPickup_latitude(Double.parseDouble(splittedText[7]));
            if (!splittedText[8].equals("")) taxidrive.setDropoff_longitude(Double.parseDouble(splittedText[8]));
            if (!splittedText[9].equals("")) taxidrive.setDropoff_latitude(Double.parseDouble(splittedText[9]));
            taxidrive.setPayment_type(splittedText[10]);
            taxidrive.setFare_amount(Double.parseDouble(splittedText[11]));
            taxidrive.setSurcharge(Double.parseDouble(splittedText[12]));
            taxidrive.setMta_tax(Double.parseDouble(splittedText[13]));
            taxidrive.setTip_amount(Double.parseDouble(splittedText[14]));
            taxidrive.setTolls_amount(Double.parseDouble(splittedText[15]));
            taxidrive.setTotal_amount(Double.parseDouble(splittedText[16]));
            out.collect(taxidrive);
        }
    }
}
