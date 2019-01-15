package com.daryl.springbootmq1.consumer;

import com.daryl.springbootmq1.util.RabbitMqUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.juli.logging.Log;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * deliveryTag（唯一标识 ID）：当一个消费者向 RabbitMQ 注册后，会建立起一个 Channel ， RabbitMQ 会用 basic.deliver
 * 方法向消费者推送消息，这个方法携带了一个 delivery tag， 它代表了 RabbitMQ 向该 Channel 投递的这条消息的唯一标识 ID，是一个单调递增的正整数，
 * delivery tag 的范围仅限于 Channel
 */

@Component
@Slf4j
public class UserRabbitMqConsumer {

    @Resource
    private RabbitTemplate rabbitTemplate;
//    @Resource
//    private MessageHandler messageHander;
    @Value("${java.rabbitmq.consumer.service.retry.exchange}")
    private String userServiceListenerRetryExchange;
    @Value("${java.rabbitmq.consumer.service.fail.exchange}")
    private String userServiceListenerFailExchange;
    @Value("${java.rabbitmq.consumer.service.user.retry.routingkey}")
    private String userSerivceRetryOrFailRoutingKey;
    private Log log;

    @SuppressWarnings("unused")
    @RabbitListener(queues = {"material@user"})
    public void consumerMessage(Message message, Channel channel) throws IOException {
        try {
            /**
             * 消费者自己做幂等
             */
//            messageHander.HandlerMessage(message, "user");
            /** 手动抛出异常,测试消息重试 */
            int i = 5 / 0;
        } catch (Exception e) {
            long retryCount = RabbitMqUtil.getRetryCount(message.getMessageProperties());
            CorrelationData correlationData =
                    new CorrelationData(message.getMessageProperties().getCorrelationId());
            Message newMessage = null;
            if (retryCount >= 3) {
                /** 如果重试次数大于3,则将消息发送到失败队列等待人工处理 */
                newMessage = RabbitMqUtil.buildMessage(message);
                try {
                    rabbitTemplate.convertAndSend(userServiceListenerFailExchange,
                            userSerivceRetryOrFailRoutingKey, newMessage, correlationData);
                    log.info("用户体系服务消费者消费消息在重试3次后依然失败，将消息发送到fail队列,发送消息:" + new String(newMessage.getBody()));
                } catch (Exception e1) {
                    log.error("用户体系服务消息在发送到fail队列的时候报错:" + e1.getMessage() + ",原始消息:"
                            + new String(newMessage.getBody()));
                }
            } else {
                newMessage = RabbitMqUtil.buildMessage2(message);
                try {
                    /** 如果当前消息被重试的次数小于3,则将消息发送到重试队列,等待重新被消费{延迟消费} */
                    rabbitTemplate.convertAndSend(userServiceListenerRetryExchange,
                            userSerivceRetryOrFailRoutingKey, newMessage, correlationData);
                    log.info("用户服务消费者消费失败，消息发送到重试队列;" + "原始消息：" + new String(newMessage.getBody()) + ";第"
                            + (retryCount+1) + "次重试");
                } catch (Exception e1) {
                    //test
                    // 如果消息在重发的时候,出现了问题,可用nack,经过开发中的实际测试，当消息回滚到消息队列时，
                    // 这条消息不会回到队列尾部，而是仍是在队列头部，这时消费者会立马又接收到这条消息，进行处理，接着抛出异常，
                    // 进行回滚，如此反复进行。这种情况会导致消息队列处理出现阻塞，消息堆积，导致正常消息也无法运行
                    // channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                    // 改为重新发送消息,经过多次重试后，如果重试次数大于3,就不会再走这，直接丢到了fail queue等待人工处理
                    log.error("消息发送到重试队列的时候，异常了:" + e1.getMessage() + ",重新发送消息");
                }
            }
        } finally {
            /**
             * 关闭rabbitmq的自动ack,改为手动ack 1、因为自动ack的话，其实不管是否成功消费了，rmq都会在收到消息后立即返给生产者ack,但是很有可能 这条消息我并没有成功消费
             * 2、无论消费成功还是消费失败,都要手动进行ack,因为即使消费失败了,也已经将消息重新投递到重试队列或者失败队列
             * 如果不进行ack,生产者在超时后会进行消息重发,如果消费者依然不能处理，则会存在死循环
             */
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            /**
             * 拒绝单条消息
             * 第一个参数deliveryTag：发布的每一条消息都会获得一个唯一的deliveryTag，deliveryTag在channel范围内是唯一的
             * 第二个参数requeue：表示如何处理这条消息，如果值为true，则重新放入RabbitMQ的发送队列，如果值为false，则通知RabbitMQ销毁这条消息
             * channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
             */
            /**
             * 丢弃这条消息, ack返回false，不重回到队列(提供一次对多条消息进行拒绝的功能)
             * 第一个参数deliveryTag：发布的每一条消息都会获得一个唯一的deliveryTag，deliveryTag在channel范围内是唯一的
             * 第二个参数multiple：批量确认标志。如果值为true，包含本条消息在内的、所有比该消息deliveryTag值小的 消息都被拒绝了（除了已经被 ack 的以外）;如果值为false，只拒绝本条消息
             * 第三个参数requeue：表示如何处理这条消息，如果值为true，则重新放入RabbitMQ的发送队列，如果值为false，则通知RabbitMQ销毁这条消息
             * channel.basicNack(message.getMessageProperties().getDeliveryTag(), false,false);
             */
        }
    }

}
