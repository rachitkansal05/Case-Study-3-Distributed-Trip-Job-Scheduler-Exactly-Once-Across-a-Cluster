package com.moveinsync.scheduler.app.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.sink.HttpEffectSink;
import com.moveinsync.scheduler.core.ports.EffectSink;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.time.Duration;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring wiring of the engine. The node (and so every loop) exists only when {@code
 * scheduler.engine.enabled} is true (the default); the test profile turns it off so API tests
 * never race the engine.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "scheduler.engine", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EngineConfiguration {

    @Bean
    @ConditionalOnMissingBean(EffectSink.class)
    HttpEffectSink httpEffectSink(SchedulerProperties props, EngineProperties engine, ObjectMapper mapper) {
        return new HttpEffectSink(URI.create(props.sink().baseUrl()), engine.sink().connectTimeout(),
                engine.sink().requestTimeout(), mapper);
    }

    @Bean
    @ConditionalOnMissingBean
    HandlerRegistry handlerRegistry(ObjectMapper mapper) {
        return HandlerRegistry.defaults(mapper);
    }

    @Bean
    SchedulerNode schedulerNode(SchedulerProperties props, EngineProperties engine, DataSource dataSource,
                                EffectSink sink, HandlerRegistry handlers, MeterRegistry meters) {
        return new SchedulerNode(EngineSettings.from(props, engine), dataSource, sink, handlers, meters);
    }

    @Bean
    SmartLifecycle schedulerNodeLifecycle(SchedulerNode node, SchedulerProperties props) {
        return new SchedulerNodeLifecycle(node, props.runLease().duration().plus(Duration.ofSeconds(5)));
    }

    /** Starts the node after the context is ready and stops it gracefully before the pool closes. */
    static final class SchedulerNodeLifecycle implements SmartLifecycle {
        private final SchedulerNode node;
        private final Duration stopTimeout;
        private volatile boolean running;

        SchedulerNodeLifecycle(SchedulerNode node, Duration stopTimeout) {
            this.node = node;
            this.stopTimeout = stopTimeout;
        }

        @Override
        public void start() {
            node.start();
            running = true;
        }

        @Override
        public void stop() {
            node.stop(stopTimeout);
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int getPhase() {
            return Integer.MAX_VALUE - 1000; // start late, stop early (before the DataSource closes)
        }
    }
}
