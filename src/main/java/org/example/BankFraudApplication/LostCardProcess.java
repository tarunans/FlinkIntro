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

public class LostCardProcess extends ProcessFunction<Tuple2< String, String >, Tuple2 < String, String > >
        implements Serializable {

    private Map<String, AlarmedCustomer> lostCardMap = new TreeMap<>();

    public LostCardProcess() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader
                ("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/lost_cards.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            AlarmedCustomer alarmedCustomer = new AlarmedCustomer(line);
            lostCardMap.put(alarmedCustomer.id, alarmedCustomer);
        }
    }

    @Override
    public void processElement(Tuple2<String, String> value,
                               ProcessFunction<Tuple2<String, String>, Tuple2<String, String>>.Context context,
                               Collector<Tuple2<String, String>> collector) throws Exception {
        lostCardMap.forEach((lostCardId,card) -> {

            // get card_id of current transaction
            final String cId = value.f1.split(",")[5];
            if (cId.equals(lostCardId)) {
                collector.collect(new Tuple2 < String, String > ("__ALARM__", "Transaction: " + value + " issued via LOST card"));
            }
        });
    }
}
