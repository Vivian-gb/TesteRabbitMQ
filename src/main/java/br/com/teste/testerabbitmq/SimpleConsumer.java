package br.com.teste.testerabbitmq;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class SimpleConsumer {

    public static void main(String[] argv) throws Exception {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      Connection connection = factory.newConnection();
      Channel channel = connection.createChannel();

      boolean durable = true;
      channel.queueDeclare(SimpleProducer.QUEUE_NAME, durable, false, false, null);
      int prefetchCount = 1;
      channel.basicQos(prefetchCount);
      System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

      final Consumer consumer = new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            String message = new String(body, "UTF-8");

            System.out.println(" [x] Received '" + message + "'");
            try {
              doWork(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
              System.out.println(" [x] Done");
              channel.basicAck(envelope.getDeliveryTag(), false);
            }
          }
        };
        boolean autoAck = false; // acknowledgment is covered below
        channel.basicConsume(SimpleProducer.QUEUE_NAME, autoAck, consumer);
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    } 
}
