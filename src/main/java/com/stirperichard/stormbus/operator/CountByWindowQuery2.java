package com.stirperichard.stormbus.operator;

/*
public class CountByWindowQuery2 extends BaseRichBolt {

    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String REASON           	= "reason";
    public static final String OCCURRED_ON_MILLIS   = "occurred_on_millis";

    public static final String DAY              = "day";
    public static final String WEEK             = "week";

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private int hour;

    private long latestCompletedTimeframeDay, latestCompletedTimeframeWeek;

    private SimpleDateFormat sdf;

    Map<String, Window> map_day_morning, map_day_afternoon, map_week_morning, map_week_afternoon;


    /*
     * QUERY 2 :
     *
     *  Fornire la classifica delle tre cause di disservizio pi`u frequenti
     * (ad esempio, Heavy Traffic, MechanicalProblem, Flat Tire)
     * nelle due fasce orarie di servizio 5:00-11:59 e 12:00-19:00.
     * Le tre cause sonoordinate dalla pi`u frequente alla meno frequente.
     *
     */
/*
    public CountByWindowQuery2() {
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.latestCompletedTimeframeDay = 0;
        this.latestCompletedTimeframeWeek = 0;
        this.map_day_afternoon = new HashMap<String, Window>();
        this.map_day_morning = new HashMap<String, Window>();
        this.map_week_morning = new HashMap<String, Window>();
        this.map_week_afternoon = new HashMap<String, Window>();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Metronome.S_METRONOME)){

            handleMetronomeMessage(input);  //sliding window based on event time

        } else {

            handleBusData(input);

        }
    }

    private void handleMetronomeMessage(Tuple tuple){
        String msgType          = tuple.getSourceStreamId();
        String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
        Long time		 		= tuple.getLongByField(Metronome.OCCURREDON_MILLIS);
        long timestamp 		    = tuple.getLongByField(Metronome.F_TIMESTAMP);
        String occurredOn   	= tuple.getStringByField(Metronome.OCCURRED_ON);

        if (msgType.equals(Metronome.METRONOME_D)) {

            long latestTimeframe = TimeUtils.roundToCompletedDay(time);

            if (this.latestCompletedTimeframeDay < latestTimeframe) {

                int elapsedDay = (int) Math.ceil((latestTimeframe - latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_day.keySet()) {

                    Window w = map_day.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedDay);
                    int numberOfProblem = w.getCounter();

                    /* Reduce memory by removing windows with no data */
/*                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(numberOfProblem);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(DAY, v);
                }

                /* Reduce memory by removing windows with no data */
/*                for (String r : expiredRoutes) {
                    map_day.remove(r);
                }

                this.latestCompletedTimeframeDay = latestTimeframe;
            }
        }

        if (msgType.equals(Metronome.METRONOME_W)) {

            long latestTimeframe = TimeUtils.lastWeek(time);

            if (this.latestCompletedTimeframeWeek < latestTimeframe) {

                int elapsedWeek = (int) Math.ceil((latestTimeframe - latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_week.keySet()) {

                    Window w = map_week.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedWeek);
                    int numberOfProblem = w.getCounter();

                    /* Reduce memory by removing windows with no data */
 /*                   expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(numberOfProblem);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(WEEK, v);
                }

                /* Reduce memory by removing windows with no data */
 /*               for (String r : expiredRoutes) {
                    map_week.remove(r);
                }

                this.latestCompletedTimeframeWeek = latestTimeframe;
            }
        }

        collector.ack(tuple);

    }

    private void handleBusData(Tuple tuple){

        String source           = tuple.getSourceStreamId();
        String reason 			= tuple.getStringByField(ParseCSV.REASON);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        int howLongDelayed	    = tuple.getIntegerByField(ParseCSV.HOW_LONG_DELAYED);
        long occurredOnMillis   = tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);

        long latestTimeframeDay     = TimeUtils.roundToCompletedDay(occurredOnMillis);
        long latestTimeframeWeek    = TimeUtils.lastWeek(occurredOnMillis);

        if(this.latestCompletedTimeframeDay == 0){
            this.latestCompletedTimeframeDay = TimeUtils.roundToCompletedHour(occurredOnMillis);
        }

        if(this.latestCompletedTimeframeWeek == 0){
            this.latestCompletedTimeframeWeek = TimeUtils.lastWeek(occurredOnMillis);
        }

        if (latestTimeframeDay > this.latestCompletedTimeframeDay) {
            int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_day_morning.keySet()) {

                Window w = map_day_morning.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedDay);
                long delayPerBoroPerDay = w.getEstimatedTotal();
                long avgPerBoroPerDay = delayPerBoroPerDay/24;  //Sommatoria giornaliera diviso il numero di ore

                /* Reduce memory by removing windows with no data */
/*                expiredRoutes.add(r);

                Values v = new Values();
                v.add(occurredOn);
                v.add(r);
                v.add(avgPerBoroPerDay);
                v.add(occurredOnMillis);
                collector.emit(DAY, v);
            }


            for (String r : map_day_afternoon.keySet()) {

                Window w = map_day_afternoon.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedDay);
                long delayPerBoroPerDay = w.getEstimatedTotal();
                long avgPerBoroPerDay = delayPerBoroPerDay/24;  //Sommatoria giornaliera diviso il numero di ore

                /* Reduce memory by removing windows with no data */
/*                expiredRoutes.add(r);

                Values v = new Values();
                v.add(occurredOn);
                v.add(r);
                v.add(avgPerBoroPerDay);
                v.add(occurredOnMillis);
                collector.emit(DAY, v);
            }

            /* Reduce memory by removing windows with no data */
/*            for (String r : expiredRoutes) {
                map_day_afternoon.remove(r);
                map_day_morning.remove(r);
            }



            this.latestCompletedTimeframeDay = latestTimeframeDay;
        }

        if (latestTimeframeWeek > this.latestCompletedTimeframeWeek) {
            int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_week.keySet()) {

                Window w = map_week.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedWeek);
                long delayPerBoroPerWeek = w.getEstimatedTotal();
                long avgDelayPerBoroPerWeek = delayPerBoroPerWeek / 7;    //Media settimanale in base giornaliera

                /* Reduce memory by removing windows with no data */
/*                expiredRoutes.add(r);

                Values v = new Values();
                v.add(occurredOn);
                v.add(r);
                v.add(avgDelayPerBoroPerWeek);
                v.add(occurredOnMillis);
                collector.emit(WEEK, v);
            }

            /* Reduce memory by removing windows with no data */
/*            for (String r : expiredRoutes) {
                map_week.remove(r);
            }

            this.latestCompletedTimeframeWeek = latestTimeframeWeek;
        }

        /* Time has not moved forward. Update and emit count */  //WINDOW DAY
/*        Window wDM = map_day_morning.get(reason);
        if (wDM == null) {
            wDM = new Window(1);
            map_day_morning.put(reason, wDM);
        }
        wDM.increment();


        /* Time has not moved forward. Update and emit count */  //WINDOW DAY
/*        Window wDA = map_day_afternoon.get(reason);
        if (wDA == null) {
            wDA = new Window(1);
            map_day_afternoon.put(reason, wDA);
        }
        wDA.increment();


        /* Time has not moved forward. Update and emit count */  //WINDOW WEEK
/*        Window wWM = map_week_morning.get(reason);
        if (wWM == null) {
            wWM = new Window(7);
            map_week_morning.put(reason, wWM);
        }
        wWM.increment();


        /* Time has not moved forward. Update and emit count */  //WINDOW WEEK
/*        Window wWA = map_week_afternoon.get(reason);
        if (wWA == null) {
            wWA = new Window(7);
            map_week_morning.put(reason, wWA);
        }
        wWA.increment();


        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, F_TIMESTAMP));
    }

}

 */
