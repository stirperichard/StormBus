package test;

import com.stirperichard.stormbus.kafka.SimpleKakfaConsumer;
import com.stirperichard.stormbus.utils.Constants;

public class TestConsumer {

    public static void main(String[] args) {
        SimpleKakfaConsumer consumer = new SimpleKakfaConsumer(1, Constants.TOPIC_3_OUTPUT);
        consumer.run();
    }
}