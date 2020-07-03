package com.stirperichard.stormbus.datasource;


import com.stirperichard.stormbus.kafka.SimpleKakfaProducer;
import com.stirperichard.stormbus.utils.Constants;
import com.stirperichard.stormbus.utils.TimeUtils;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Datasource {

    public static void main(String[] args) {

        SimpleKakfaProducer producer = new SimpleKakfaProducer(Constants.TOPIC_1_INPUT);

        BufferedReader br = null;
        String line = "";

        long lastTs = 0;

        try {

            br = new BufferedReader(new FileReader(Constants.DATASET));

            String header = br.readLine();
            String firstLine = br.readLine();
            long eventTime = getEventTime(firstLine);

            producer.produce(null, firstLine);

            int k = 0;
            while ((line = br.readLine()) != null && k < 3000) {
                {
                    long newTs = getEventTime(line);
                    if (lastTs > 0) {
                        // we have to sleep SECONDS_PER_TIME_UNIT
                        // for each TIME_UNIT_IN_SECONDS passed from last tuple
                        // to this one
                        long fromTupleToSystemTime = Constants.TIME_UNIT_IN_SECONDS * Constants.SECONDS_PER_TIME_UNIT;
                        long sleepTime = (newTs - lastTs) /  fromTupleToSystemTime;
                        System.out.println("Sleep for: \u001B[31m" + sleepTime + "\u001B[0m");
                        Utils.sleep(sleepTime);
                    }
                    lastTs = newTs;

                    producer.produce(null, line);
                    k++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static long getEventTime(String line) {
        long ts = 0;
        String[] tokens = line.split(";");
        ts = TimeUtils.millisFromTimeStamp(tokens[7]);
        return ts;

    }

}