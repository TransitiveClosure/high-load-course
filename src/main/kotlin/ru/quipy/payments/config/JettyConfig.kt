package ru.quipy.payments.config

import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JettyConfig {
    @Bean
    fun jettyServletWebServerFactory(): JettyServletWebServerFactory {
        val factory = JettyServletWebServerFactory()

        factory.addServerCustomizers(JettyServerCustomizer { server ->
            server.connectors.forEach { connector ->
                val h2cFactory = connector.getConnectionFactory("h2c") as? HTTP2CServerConnectionFactory
                h2cFactory?.let {
                    it.maxConcurrentStreams = 1_000_000
                }
            }
        })

        return factory
    }
}