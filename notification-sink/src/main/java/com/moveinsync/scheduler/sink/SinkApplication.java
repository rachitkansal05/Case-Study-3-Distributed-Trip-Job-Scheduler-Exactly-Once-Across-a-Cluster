package com.moveinsync.scheduler.sink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Mock notification provider. Idempotent by {@code Idempotency-Key}, fencing-aware by {@code Fencing-Token}. */
@SpringBootApplication
public class SinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(SinkApplication.class, args);
    }
}
