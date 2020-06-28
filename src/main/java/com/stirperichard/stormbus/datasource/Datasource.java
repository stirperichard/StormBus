package com.stirperichard.stormbus.datasource;


import com.stirperichard.stormbus.kafka.SimpleKakfaProducer;
import com.stirperichard.stormbus.query3.Configuration;
import com.stirperichard.stormbus.utils.Constants;
import com.stirperichard.stormbus.utils.TimeUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Datasource {

    public static void main(String[] args) {

        SimpleKakfaProducer producer = new SimpleKakfaProducer(Configuration.TOPIC_1_INPUT);

        BufferedReader br = null;
        String line = "";

        try {

            br = new BufferedReader(new FileReader(Constants.DATASET));

            String header = br.readLine();
            String firstLine = br.readLine();
            long eventTime = getEventTime(firstLine);

            producer.produce(null, firstLine);

            int k = 0;
            while ((line = br.readLine()) != null && k < 3000) {
                {
                    long actualEventTime = getEventTime(line);
                    long diff = (actualEventTime - eventTime);// /1000
                    Thread.sleep(diff);
                    eventTime = actualEventTime;
                    producer.produce(null, line);
                    k++;
                }
            }
        } catch (IOException | InterruptedException e) {
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