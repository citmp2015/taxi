package org.tuberlin.de.paymenttype_tip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;
/**
 * Created by Fabian on 24.10.2015.
 */
public class PaymentType_Tip {

  public static void main(String args[]) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Taxidrive> taxidriveDataSet = Job.readInput(env, args[0]);

    Tuple2<Double, Integer> tipCountCash = taxidriveDataSet
                    .filter(drive -> drive.getPayment_type().equals("CSH"))
                    .flatMap(new FlatMapFunction<Taxidrive, Tuple2<Double, Integer>>() {
                      @Override public void flatMap(Taxidrive value,
                                      Collector<Tuple2<Double, Integer>> out)
                                      throws Exception {
                        out.collect(new Tuple2<>(value.getTip_amount(), 1));
                      }
                    })
                    .reduce((x,y) -> new Tuple2<Double, Integer>(x.f0 + y.f0, x.f1 + y.f1))
                    .collect().get(0);
    Double averageTipCash = tipCountCash.f0 / tipCountCash.f1;

    Tuple2<Double, Integer> amountCountCash = taxidriveDataSet
                    .filter(drive -> drive.getPayment_type().equals("CSH"))
                    .flatMap(new FlatMapFunction<Taxidrive, Tuple2<Double, Integer>>() {
                      @Override public void flatMap(Taxidrive value,
                                      Collector<Tuple2<Double, Integer>> out)
                                      throws Exception {
                        out.collect(new Tuple2<>(value.getTotal_amount(), 1));
                      }
                    })
                    .reduce((x,y) -> new Tuple2<Double, Integer>(x.f0 + y.f0, x.f1 + y.f1))
                    .collect().get(0);
    Double averageAmountCash = amountCountCash.f0 / amountCountCash.f1;

    Tuple2<Double, Integer> tipCountCard = taxidriveDataSet
                    .filter(drive -> drive.getPayment_type().equals("CRD"))
                    .flatMap(new FlatMapFunction<Taxidrive, Tuple2<Double, Integer>>() {
                      @Override public void flatMap(Taxidrive value,
                                      Collector<Tuple2<Double, Integer>> out)
                                      throws Exception {
                        out.collect(new Tuple2<>(value.getTip_amount(), 1));
                      }
                    })
                    .reduce((x,y) -> new Tuple2<Double, Integer>(x.f0 + y.f0, x.f1 + y.f1))
                    .collect().get(0);
    Double averageTipCard = tipCountCard.f0 / tipCountCard.f1;

    Tuple2<Double, Integer> amountCountCard = taxidriveDataSet
                    .filter(drive -> drive.getPayment_type().equals("CRD"))
                    .flatMap(new FlatMapFunction<Taxidrive, Tuple2<Double, Integer>>() {
                      @Override public void flatMap(Taxidrive value,
                                      Collector<Tuple2<Double, Integer>> out)
                                      throws Exception {
                        out.collect(new Tuple2<>(value.getTotal_amount(), 1));
                      }
                    })
                    .reduce((x,y) -> new Tuple2<Double, Integer>(x.f0 + y.f0, x.f1 + y.f1))
                    .collect().get(0);
    Double averageAmountCard = amountCountCard.f0 / amountCountCard.f1;

    System.out.println("Average Tip Cash: " + averageTipCash + " Average Amount Cash: " + averageAmountCash + " Average Tip Card: " + averageTipCard + " Average Amount Card: " + averageAmountCard);

  }
}
