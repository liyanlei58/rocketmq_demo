package com.lyl.mq.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * http://rocketmq.apache.org/docs/filter-by-sql92-example/
 * In most cases, tag is a simple and useful design to select message you want. For example:
 *
 * DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_EXAMPLE");
 * consumer.subscribe("TOPIC", "TAGA || TAGB || TAGC");
 *
 * The consumer will recieve messages that contains TAGA or TAGB or TAGC. But the limitation is that one message only can have one tag, and this may not work for sophisticated scenarios. In this case, you can use SQL expression to filter out messages.
 * Principle
 * SQL feature could do some calculation through the properties you put in when sending messages. Under the grammars defined by RocketMQ, you can implement some interesting logic. Here is an example:
 *
 * ------------
 * | message  |
 * |----------|  a > 5 AND b = 'abc'
 * | a = 10   |  --------------------> Gotten
 * | b = 'abc'|
 * | c = true |
 * ------------
 * ------------
 * | message  |
 * |----------|   a > 5 AND b = 'abc'
 * | a = 1    |  --------------------> Missed
 * | b = 'abc'|
 * | c = true |
 * ------------
 * Grammars
 * RocketMQ only defines some basic grammars to support this feature. You could also extend it easily.
 *
 * Numeric comparison, like >, >=, <, <=, BETWEEN, =;
 * Character comparison, like =, <>, IN;
 * IS NULL or IS NOT NULL;
 * Logical AND, OR, NOT;
 * Constant types are:
 *
 * Numeric, like 123, 3.1415;
 * Character, like ‘abc’, must be made with single quotes;
 * NULL, special constant;
 * Boolean, TRUE or FALSE;
 * Usage constraints
 * Only push consumer could select messages by SQL92. The interface is:
 *
 * public void subscribe(final String topic, final MessageSelector messageSelector)
 *
 * Producer example
 * You can put properties in message through method putUserProperty when sending.
 */
public class FilterProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("FilterProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName("FilterProducer");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 10; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("FilterTopic" /* Topic */,
                    "TagA" /* Tag */,
                    ("SyncProducer Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // Set some properties.
            msg.putUserProperty("a", String.valueOf(i));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}