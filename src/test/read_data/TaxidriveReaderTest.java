package read_data;

import junit.framework.TestCase;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.util.Collector;
import org.tuberlin.de.read_data.Job;
import org.tuberlin.de.read_data.Taxidrive;

import java.util.LinkedList;

/**
 * Created by Fabian on 20.10.2015.
 */
public class TaxidriveReaderTest extends TestCase {

  public void testReader() throws Exception {
    String inputString = "22D70BF00EEB0ADC83BA8177BB861991,3FF2709163DE7036FCAA4E5A3324E4BF,2013-01-01 00:02:00,2013-01-01 00:02:00,0,0.00,0.000000,0.000000,0.000000,0.000000,CSH,27.00,0.00,0.50,0.00,0.00,27.50";
    LinkedList<Taxidrive> listTaxidrives = new LinkedList<>();
    Collector<Taxidrive> collectedTaxidrives = new ListCollector<>(listTaxidrives);

    new Job.TaxidriveReader().flatMap(inputString, collectedTaxidrives);

    Taxidrive createdTaxidrive = listTaxidrives.getFirst();
    assertEquals(createdTaxidrive.getPayment_type(), "CSH");
    assertEquals(createdTaxidrive.getTotal_amount(), 27.5);
  }
}
