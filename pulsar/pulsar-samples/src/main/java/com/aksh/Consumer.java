package com.aksh;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.reader.AvroReader;
import org.apache.pulsar.shade.org.apache.avro.io.Decoder;
import org.apache.pulsar.shade.org.apache.avro.io.DecoderFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Consumer {

    public static void main(String[] args) throws Exception{
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();\

        curl -X GET "localhost:9200/_search?pretty" -H 'Content-Type: application/json' -d'
        {
            "query": {
            "query_string": {
                "query": "(new york city) OR (big apple)",
                        "default_field": "content"
            }
        }
        }
        '

        http://ec2-54-82-239-29.compute-1.amazonaws.com:9200/pocdb.customers/_search?q=gmail.updated

        org.apache.pulsar.client.api.Consumer consumer = client.newConsumer()
                .topic("data-pocdb.products")
                .subscriptionName("my-subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        while (true) {
            // Wait for a message
            Message msg = consumer.receive();

            try {
                // Do something with the message
                byte[] data=msg.getData();
                System.out.printf("Message received: %s", new String(data));
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                org.apache.avro.generic.GenericRecord avroRecord = datumReader.read(
                        null,
                        decoder);

                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }


    }
}
