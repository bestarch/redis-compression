package com.bestarch.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Scanner;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.core.RedisTemplate;

import com.bestarch.demo.service.CompressionLZ4Service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

@SpringBootApplication
public class CompressionDemoApplication implements CommandLineRunner {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	RedisConnectionFactory redisConnectionFactory;

	@Autowired
	private ResourceLoader resourceLoader;
	
	@Autowired
	private CompressionLZ4Service compressionLZ4Service;
	
	//@Value()
	Integer recordCount=10;
	
	

	public static void main(String[] args) {
		SpringApplication.run(CompressionDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		JedisConnection jedisConnection = (JedisConnection) redisConnectionFactory.getConnection();
		Jedis jedis = jedisConnection.getJedis();
		init(jedis);
		compressionLZ4Service.scanAndCompress();
		
	}
	
	
	private void init(Jedis jedis) {
		PrimitiveIterator.OfInt iterator = IntStream.range(0, recordCount).iterator();
		String prefix = "_SAM_:";
		try {
			String str1 = "{\"PK\":\"123\",\"SK\":\"ABC#123\",\"assetId\":\"123\",\"totalDuration\":1000000,\"position\":100000,\"created_at\":1658910745098,\"updated_at\":1658910745098,\"isWatching\":true,\"isOnAir\":true,\"language\":\"\",\"Data\":\"ABC#123\",\"episodeNumber\":1,\"showId\":123,\"seasonId\":123}";
			String str2 = "{\"PK\":\"456\",\"SK\":\"ABC#231\",\"assetId\":\"124\",\"totalDuration\":1000000,\"position\":100000,\"created_at\":1658910745098,\"updated_at\":1658910745098,\"isWatching\":true,\"isOnAir\":true,\"language\":\"\",\"Data\":\"ABC#325\",\"episodeNumber\":2,\"showId\":123,\"seasonId\":123}";
			String str3 = "This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.";

			Map<String, Double> map1 = Map.of("XYZ#{123}+789", 10d, "XYZ#{345}+123", 20d);
			Map<String, String> map2 = Map.of("cluster", "{\"cluster_id\": 00}", "profile", "{\"age\": \"Adult\", \"gender\": \"gender\", \"top_genre\": [\"genre\"], \"top_region\": [\"Region\"]}");
			Map<String, String> map3 = Map.of("shortinfodata", "{\"0000\": {\"stackName\": \"cluster_00_stack_1\", \"description\": \"This is cluster description\", \"firstContent\": 11111111111, \"stackScore1\": 11111, \"stackScore2\": 22222, \"stackScore3\": 33333, \"hashvalue\": \"132323234hjhfj83923832nksf900sfs\", \"hashtimestamp\": \"12222222222222\"}}", "reco", "{\"0000\": [1111111111, 22222222222, 3333333333333, 444444444444, 555555555555]}");
			
			Map<String, String> map4 = Map.of("cos", "{\"livo\": \"111111111111,222222222222\", \"livs\": \"111111111111,222222222222\"}", 
					"profile", "{\"top_genre\": [\"genre\"], \"top_language\": [\"language\"], \"top_timecategory\": null, \"top_region\": [\"country\"], \"watched\": [{\"user_category\": \"original\", \"content\": [\"11111111111_6\"]}]}", 
					"clb", "{\"livo\": \"111111111111,222222222222\", \"livs\": \"111111111111,222222222222\"}", 
					"sri", "{\"livo\": \"111111111111,222222222222\", \"livs\": \"111111111111,222222222222\"}",
					"cldlng","{\"livo\": \"111111111111,222222222222\", \"livs\": \"111111111111,222222222222\"}");
			
			Map<String, String> map5 = Map.of("livo", "{\"language1\":[111111111111,222222222222],\"language2\":[111111111111,222222222222]}",
					"sport", "{\"language1\":[111111111111,222222222222]}", 
					"s", "{\"language1\":[111111111111,222222222222],\"language2\":[111111111111,222222222222]}",
					"livs", "{\"language1\":[111111111111,222222222222],\"language2\":[111111111111,222222222222]}", 
					"mov", "{\"language1\":[111111111111,222222222222],\"language2\":[111111111111,222222222222]}");
			
			Map<String, String> map6 = Map.of("shortinfodata", "{\"0000\": {\"stackName\": \"stack_1\", \"description\": \"This is stack description\", \"firstContent\": 11111111111, \"stackScore1\": 11111, \"stackScore2\": 22222, \"stackScore3\": 33333, \"hashvalue\": \"132323234hjhfj83923832nksf900sfs\", \"hashtimestamp\": \"12222222222222\"}}", 
					"reco", "{\"0000\": [1111111111, 22222222222, 3333333333333, 444444444444, 555555555555]}");
			
			Map<String, String> map7 = Map.of("livo", "{\"language1\": [11111111111], \"language2\": [2222222222], \"language3\": [3333333333]}",
					"s", "{\"language1\": [11111111111], \"language2\": [2222222222], \"language3\": [3333333333]}", 
					"livs", "{\"language1\": [11111111111], \"language2\": [2222222222], \"language3\": [3333333333]}", 
					"mov", "{\"language1\": [11111111111], \"language2\": [2222222222], \"language3\": [3333333333]}");

			while (iterator.hasNext()) {
				int nextInt = iterator.nextInt();
				jedis.zadd(prefix.concat("ABC#123:" + nextInt), map1);
				jedis.set(prefix.concat("XYZ#{123}+789:"+nextInt), str1);
				jedis.set(prefix.concat("XYZ#{345}+123:"+nextInt), str2);
				jedis.hset(prefix.concat("sh_0000000000000000000_000000000:"+nextInt), map2);
				jedis.hset(prefix.concat("sh_cluster_00:"+nextInt), map3);
				jedis.hset(prefix.concat("0000000000000000000_000000000:"+nextInt), map4);
				jedis.hset(prefix.concat("country_region_age_gender:"+nextInt), map5);
				jedis.hset(prefix.concat("sh_region_platform:"+nextInt), map6);
				jedis.hset(prefix.concat("country_region_platform:"+nextInt), map7);
				jedis.set(prefix.concat("DUMMY_TEXT:"+nextInt), str3);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	@Deprecated
	/**
	 * Not working
	 */
	private void executeCommand() {
		JedisConnection jedisConnection = (JedisConnection) redisConnectionFactory.getConnection();
		Jedis jedis = jedisConnection.getJedis();
		try {
			Resource resource = resourceLoader.getResource("classpath:master_data.txt");
			InputStream inputStream = resource.getInputStream();
			Scanner scanner = new Scanner(inputStream);
			while (scanner.hasNextLine()) {
				String command = scanner.nextLine().trim();
				String[] args = command.split("\\s+");
				if (!command.isEmpty()) {
					try { 
						Object response = jedis.sendCommand(Protocol.Command.valueOf(args[0].toUpperCase()), Arrays.copyOfRange(args, 1, args.length));					
						System.out.println("Command: " + command + "\nResponse: " + response + "\n");
					} catch (Exception e) {
						System.err.println("Failed to execute command: " + command + "\nError: " + e.getMessage() + "\n");
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
