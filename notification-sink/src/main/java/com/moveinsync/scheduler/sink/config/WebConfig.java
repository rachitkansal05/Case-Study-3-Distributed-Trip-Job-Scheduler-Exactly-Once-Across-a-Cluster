package com.moveinsync.scheduler.sink.config;

import com.moveinsync.scheduler.sink.web.AdminTokenInterceptor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Protects {@code /admin/**} with the static admin token. */
@Configuration
@EnableConfigurationProperties(SinkProperties.class)
public class WebConfig implements WebMvcConfigurer {

    private final SinkProperties properties;

    public WebConfig(SinkProperties properties) {
        this.properties = properties;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new AdminTokenInterceptor(properties.admin().token())).addPathPatterns("/admin/**");
    }
}
