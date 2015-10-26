package org.tuberlin.de.district_per_hour;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

@SuppressWarnings("serial")
public class DistrictPerHour {
    private static final class ExtendedTaxidrive extends Taxidrive {
        public int count;

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    public static final class TaxidriveCounter implements ReduceFunction<ExtendedTaxidrive> {
        @Override
        public ExtendedTaxidrive reduce(ExtendedTaxidrive in1, ExtendedTaxidrive in2) {
            ExtendedTaxidrive taxidrive = new ExtendedTaxidrive();
            taxidrive.setTaxiID(in1.getTaxiID());
            taxidrive.setLicenseID(in1.getLicenseID());
            taxidrive.setPickup_datetime(in1.getPickup_datetime());
            taxidrive.setDropoff_datetime(in1.getDropoff_datetime());
            taxidrive.setTrip_time_in_secs(in1.getTrip_time_in_secs());
            taxidrive.setTrip_distance(in1.getTrip_distance());
            taxidrive.setPickup_longitude(in1.getPickup_longitude());
            taxidrive.setPickup_latitude(in1.getPickup_latitude());
            taxidrive.setDropoff_longitude(in1.getDropoff_longitude());
            taxidrive.setDropoff_latitude(in1.getDropoff_latitude());
            taxidrive.setPayment_type(in1.getPayment_type());
            taxidrive.setFare_amount(in1.getFare_amount());
            taxidrive.setSurcharge(in1.getSurcharge());
            taxidrive.setMta_tax(in1.getMta_tax());
            taxidrive.setTip_amount(in1.getTip_amount());
            taxidrive.setTolls_amount(in1.getTolls_amount());
            taxidrive.setTotal_amount(in1.getTotal_amount());
            taxidrive.setCount(in1.getCount() + in2.getCount());
            return taxidrive;
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textInput =  env.readTextFile(args[0]);
        DataSet<ExtendedTaxidrive> taxidriveDataSet = textInput.flatMap(new FlatMapFunction<String, ExtendedTaxidrive>() {

            @Override
            public void flatMap(String value, Collector<ExtendedTaxidrive> collector) throws Exception {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                ExtendedTaxidrive taxidrive = new ExtendedTaxidrive();


                String[] splittedText = value.split(",");
                taxidrive.setTaxiID(splittedText[0]);
                taxidrive.setLicenseID(splittedText[1]);
                taxidrive.setPickup_datetime(splittedText[2]);
                taxidrive.setDropoff_datetime(splittedText[3]);
                taxidrive.setTrip_time_in_secs(Integer.parseInt(splittedText[4]));
                taxidrive.setTrip_distance(Double.parseDouble(splittedText[5]));
                taxidrive.setPickup_longitude(Double.parseDouble(splittedText[6]));
                taxidrive.setPickup_latitude(Double.parseDouble(splittedText[7]));
                taxidrive.setDropoff_longitude(Double.parseDouble(splittedText[8]));
                taxidrive.setDropoff_latitude(Double.parseDouble(splittedText[9]));
                taxidrive.setPayment_type(splittedText[10]);
                taxidrive.setFare_amount(Double.parseDouble(splittedText[11]));
                taxidrive.setSurcharge(Double.parseDouble(splittedText[12]));
                taxidrive.setMta_tax(Double.parseDouble(splittedText[13]));
                taxidrive.setTip_amount(Double.parseDouble(splittedText[14]));
                taxidrive.setTolls_amount(Double.parseDouble(splittedText[15]));
                taxidrive.setTotal_amount(Double.parseDouble(splittedText[16]));
                taxidrive.setCount(1);

                collector.collect(taxidrive);
            }
        });

        DataSet<ExtendedTaxidrive> counts = taxidriveDataSet.groupBy("pickup_datetime").reduce(new TaxidriveCounter());
        counts.print();
        env.execute("Flink Java API Skeleton");
    }
}
