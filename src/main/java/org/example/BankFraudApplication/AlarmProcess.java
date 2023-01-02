package org.example.BankFraudApplication;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class AlarmProcess extends ProcessFunction <Tuple2 < String, String >, Tuple2 < String, String > >
 implements Serializable {

    private Map<String, AlarmedCustomer> alarmedCustomerMap = new TreeMap<>();

    public AlarmProcess() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader
                ("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/alarmed_cust.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            AlarmedCustomer alarmedCustomer = new AlarmedCustomer(line);
            alarmedCustomerMap.put(alarmedCustomer.id, alarmedCustomer);
        }
    }

    @Override
    public void processElement(Tuple2<String, String> input,
                               ProcessFunction<Tuple2<String, String>, Tuple2<String, String>>.Context context,
                               Collector<Tuple2<String, String>> out) throws Exception {
        alarmedCustomerMap.forEach((alarmedCustId, value) -> {
            // get customer_id of current transaction
            final String tId = input.f1.split(",")[3];
            if (tId.equals(alarmedCustId)) {
                out.collect(new Tuple2<String, String>("____ALARM___", "Transaction: " + input + " " +
                        "is by an ALARMED customer"));
            }
        });
    }
}
