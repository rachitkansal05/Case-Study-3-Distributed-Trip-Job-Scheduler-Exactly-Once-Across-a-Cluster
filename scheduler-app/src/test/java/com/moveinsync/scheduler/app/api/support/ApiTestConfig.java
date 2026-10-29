package com.moveinsync.scheduler.app.api.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Beans shared by all API integration tests. The {@code @Primary} fake {@code FireTimeService}
 * wins over the production fallback regardless of bean registration order.
 */
@TestConfiguration(proxyBeanMethods = false)
public class ApiTestConfig {

    @Bean
    @Primary
    FakeFireTimeService fakeFireTimeService() {
        return new FakeFireTimeService();
    }
}
