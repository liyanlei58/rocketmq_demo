package com.lyl.mq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Why batch?
 * Sending messages in batch improves performance of delivering small messages.
 *
 * Usage constraints
 * Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support.
 *
 * Besides, the total size of the messages in one batch should be no more than 1MiB.
 * How to use batch
 * If you just send messages of no more than 1MiB at a time, it is easy to use batch:
 */
public class BatchProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName("BatchProducer");
        //Launch the instance.
        producer.start();
        String topic = "BatchTopic";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID003", "Hello world 2".getBytes()));
        try {
            producer.send(messages);
        } catch (Exception e) {
            e.printStackTrace();
            //handle the error
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}