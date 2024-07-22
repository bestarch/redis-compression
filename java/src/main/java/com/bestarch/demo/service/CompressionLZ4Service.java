package com.bestarch.demo.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

@Service
public class CompressionLZ4Service {

	@Autowired
	RedisConnectionFactory redisConnectionFactory;

	public void scanAndCompress() throws IOException {
		JedisConnection jedisConnection = (JedisConnection) redisConnectionFactory.getConnection();
		Jedis jedis = jedisConnection.getJedis();

		String cursor = "0";
		ScanParams scanParams = new ScanParams().match("_SAM_:*").count(500);
						
		do {
			ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
			cursor = scanResult.getCursor();
			List<String> keys = scanResult.getResult();

			for (String key : keys) {
				String keytype = jedis.type(key);
				System.out.println("Found key: " + key);
				switch (keytype) {
					case "hash" -> {
						Map<String, String> map = jedis.hgetAll(key);
						for (String k : map.keySet()) {
							byte[] compressedBytes = compress(map.get(k));
							// jedis.hdel(key, k);
							jedis.hset(key.getBytes(), Map.of(k.getBytes(), compressedBytes));
						}
					}
					case "zset" -> {
						List<Tuple> tuples = jedis.zrangeWithScores(key, 0, -1);
						for (Tuple t : tuples) {
							double score = t.getScore();
							byte[] compressedBytes = compress(t.getElement());
							jedis.zrem(key, t.getElement());
							jedis.zadd(key.getBytes(), score, compressedBytes);
						}
					}
					case "string" -> {
						String val = jedis.get(key);
						byte[] compressedBytes = compress(val);
						jedis.set(key.getBytes(), compressedBytes);
					}
			  }
			}
		} while (!cursor.equals("0"));

	}

	private byte[] compress(String data) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		try (BlockLZ4CompressorOutputStream lz4OutputStream = new BlockLZ4CompressorOutputStream(
				byteArrayOutputStream)) {
			lz4OutputStream.write(data.getBytes(StandardCharsets.UTF_8));
			// lz4OutputStream.write(data);
		}
		byte[] resp = byteArrayOutputStream.toByteArray();
		return resp;
	}
	
	
	private byte[] decompress(byte[] data) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		try (BlockLZ4CompressorOutputStream lz4OutputStream = new BlockLZ4CompressorOutputStream(
				byteArrayOutputStream)) {
			lz4OutputStream.write(data.getBytes(StandardCharsets.UTF_8));
			// lz4OutputStream.write(data);
		}
		byte[] resp = byteArrayOutputStream.toByteArray();
		return resp;
	}

}
