package br.com.teste.testerabbitmq;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class SimpleProducer {
    public final static String QUEUE_NAME = "fila";
    public static void main(String[] args) throws IOException, TimeoutException {
        //create a connection to the server
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        boolean durable = true;
        //declare a queue for us to send to
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        
        //publish a message to the queue
        for (int  i = 0;i < 10; i++) {
            String message = getMessage(i);
            sendMessage(channel, message);
        }
        
        //close the channel and the connection
        channel.close();
        connection.close();
    }

    private static void sendMessage(Channel channel, String message) throws IOException {
        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }
    
    private static String getMessage(int i){
        return UUID.randomUUID().toString()+ ((i%2==0) ? ".........." : ".");
    }
}
