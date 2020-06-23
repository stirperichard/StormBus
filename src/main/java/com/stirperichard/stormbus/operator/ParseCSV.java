package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.enums.Reason;
import com.stirperichard.stormbus.utils.Constants;
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

import static com.stirperichard.stormbus.utils.ParseTime.minutesDelayed;

public class ParseCSV extends BaseRichBolt {

    public static final String F_MSGID = "MSGID";
    public static final String OCCURRED_ON = "occurredOn";
    public static final String BORO = "boro";
    public static final String REASON = "reason";
    public static final String HOW_LONG_DELAYED = "howLongDelayed";
    public static final String BUS_COMPANY_NAME = "busCompanyName";
    public static final String F_TIMESTAMP = "timestamp";
    public static final String OCCURRED_ON_MILLIS = "occurred_on_millis";
    public static final String DAY_IN_MONTH = "day_in_month";

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
        String rawData = input.getStringByField(DataGeneratorQ1.DATA);
        long occurredOnMillis = input.getLongByField(DataGeneratorQ1.OCCURRED_ON);
        int ID = input.getIntegerByField(DataGeneratorQ1.ID);

        if (ID > prevID) {
            prevID = ID;
            /* Do NOT emit if the EOF has been reached */
            if (rawData == null || rawData.equals(Constants.REDIS_EOF)) {
                collector.ack(input);
                return;
            }


            /* Do NOT emit if the EOF has been reached */
            String[] data = rawData.split(";", -1);
            if (data == null || data.length != 21) {
                collector.ack(input);
                return;
            }

            System.out.println("\u001B[31m" + rawData + "\u001B[0m");

            Values values = new Values();

            //AGGIUNGO ID MESSAGGIO
            values.add(ID);

            if (!data[5].isEmpty()) {
                //Aggiungo la Reason
                values.add(mappingReason(data[5]));
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
            if (!data[9].isEmpty()) {
                values.add(data[9]);
            } else {
                collector.ack(input);
                return;
            }


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
        outputFieldsDeclarer.declare(new Fields(F_MSGID, REASON, OCCURRED_ON, BORO, BUS_COMPANY_NAME, HOW_LONG_DELAYED, OCCURRED_ON_MILLIS, DAY_IN_MONTH));
    }

    public static Reason mappingReason(String reason) {
        if (reason.equals("Accident")) {
            return Reason.ACCIDENT;
        } else if (reason.equals("Delayed by School")) {
            return Reason.DELAYED_BY_SCHOOL;
        } else if (reason.equals("Flat Tire")) {
            return Reason.FLAT_TIRE;
        } else if (reason.equals("Heavy Traffic")) {
            return Reason.HEAVY_TRAFFIC;
        } else if (reason.equals("Mechanical Problem")) {
            return Reason.MECHANICAL_PROBLEM;
        } else if (reason.equals("Problem Run")) {
            return Reason.PROBLEM_RUN;
        } else if (reason.equals("Weather Condition")) {
            return Reason.WEATHER_CONDITION;
        } else if (reason.equals("Won't Start")) {
            return Reason.WONT_START;
        } else {
            return Reason.OTHER;
        }
    }
}
