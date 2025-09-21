package com.Jeff.kafkastream;

import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;

public class LogProcessor implements Processor<byte[], byte[], byte[], byte[]> {

    private ProcessorContext<byte[], byte[]> context;
    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
        this.context = context;
    }

    @Override
    public void process(Record<byte[], byte[]> record) {
        // turn log info into String
        String input = new String(record.value(), StandardCharsets.UTF_8);

        // check if it has "MOVIE_RATING_PREFIX"
        if( input.contains("MOVIE_RATING_PREFIX:")){
            System.out.println("movie rating data coming! >>>>>>>>>>>" + input);

            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();


            Record<byte[], byte[]> newRecord =
                    new Record<>("logProcessor".getBytes(StandardCharsets.UTF_8),
                            input.getBytes(StandardCharsets.UTF_8),
                            record.timestamp());
            context.forward(newRecord);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
