package com.bestarch.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class RedisConfig {

	@Value("${redis.url:localhost}")
	private String url;

	@Value("${redis.port:6379}")
	private Integer port;

	@Value("${redis.password:admin}")
	private String password;

	@Bean
	public RedisConnectionFactory redisConnectionFactory() {
		RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration(url, port);
		redisStandaloneConfiguration.setPassword(RedisPassword.of(password));
		RedisConnectionFactory redisConnectionFactory = new JedisConnectionFactory(redisStandaloneConfiguration);
		return redisConnectionFactory;
	}

	@Bean
	RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, Object> template = new RedisTemplate<>();
		//template.setDefaultSerializer(StringRedisSerializer.UTF_8);
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();
		return template;
	}
	

}
