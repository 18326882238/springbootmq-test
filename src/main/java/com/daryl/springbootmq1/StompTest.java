package com.daryl.springbootmq1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class StompTest {

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("rabbitmq", "fanout");
        String routingKey = "rabbitmq_routingkey";
        String message = "{\"name\":\"推送测试数据!\"}";
        channel.basicPublish("rabbitmq", routingKey,null, message.getBytes());
        System.out.println("[x] Sent Message:"+message);
        channel.close();
        connection.close();
    }

}
