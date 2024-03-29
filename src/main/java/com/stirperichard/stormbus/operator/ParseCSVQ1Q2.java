package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.*;
import static com.stirperichard.stormbus.utils.ParseTime.minutesDelayed;

public class ParseCSVQ1Q2 extends BaseRichBolt {

    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public static int prevID;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple input) {
        String rawData = input.getStringByField(RAW_DATA);

        if (rawData == null) {
            collector.ack(input);
            return;
        }


        /* Do NOT emit if the EOF has been reached */
        String[] data = rawData.split(";", -1);
        if (data == null || data.length != 21) {
            collector.ack(input);
            return;
        }

        int id = Integer.parseInt(data[1]);

        if (id > prevID) {
            prevID = id;
            /* Do NOT emit if the EOF has been reached */

            Values values = new Values();

            //AGGIUNGO ID MESSAGGIO
            values.add(id);

            if (!data[5].isEmpty()) {
                //Aggiungo la Reason
                //values.add(mappingReason(data[5]));
                values.add(data[5]);
            } else {
                collector.ack(input);
                return;
            }

            //Controllo la validità e aggiungo il valore della varibile Occurred_On (campo 7)
            if (!data[7].isEmpty()) {
                values.add(data[7]);
            } else {
                collector.ack(input);
                return;
            }

            //Controllo la validità e aggiungo il valore della varibile Boro (campo 9)
            values.add(data[9]);


            //Controllo la validità e aggiungo il valore della varibile Bus_Company_Name (campo 10)
            if (!data[10].isEmpty()) {
                values.add(data[10]);
            } else {
                collector.ack(input);
                return;
            }

            //Controllo la validità e setto la varibile How_Long_Delayed (campo 11)
            if (!data[11].isEmpty()) {
                values.add(minutesDelayed(data[11]));
            } else {
                values.add(0);
            }

            //Inserisco l'occurredOn in millisecondi
            Date dDate;
            try {
                dDate = sdf.parse(data[7]);
                //OccurredOn Millis
                values.add(dDate.getTime());

            } catch (ParseException e) {
                e.printStackTrace();
                collector.ack(input);
                return;
            }


            //Aggiungo i giorni nel mese.
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(dDate.getTime());
            int dayMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);

            values.add(dayMonth);

            collector.ack(input);
            collector.emit(values);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, REASON, OCCURRED_ON, BORO, BUS_COMPANY_NAME, HOW_LONG_DELAYED,
                OCCURREDON_MILLIS, DAY_IN_MONTH));
    }

}
