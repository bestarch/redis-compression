package com.bestarch.demo.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

@Service
public class DecompressionLZ4Service {

	@Autowired
	RedisConnectionFactory redisConnectionFactory;

	public void scanAndDecompress() throws IOException {
		JedisConnection jedisConnection = (JedisConnection) redisConnectionFactory.getConnection();
		Jedis jedis = jedisConnection.getJedis();

		byte[] cursor = new byte[] {0};
		ScanParams scanParams = new ScanParams().match("_SAM_:*").count(500);
						
		do {
			ScanResult<byte[]> scanResult = jedis.scan(cursor, scanParams);
			cursor = scanResult.getCursorAsBytes();
			List<byte[]> keys = scanResult.getResult();

			for (byte[] key : keys) {
				String keytype = jedis.type(key);
				System.out.println("Found key: " + key);
				switch (keytype) {
					case "hash" -> {
						Map<byte[], byte[]> map = jedis.hgetAll(key);
						for (byte[] k : map.keySet()) {
							byte[] decompressedBytes = decompress(map.get(k));
							jedis.hset(key, Map.of(k, decompressedBytes));
						}
					}
					case "zset" -> {
						List<Tuple> tuples = jedis.zrangeWithScores(key, 0, -1);
						jedis.del(key);
						for (Tuple t : tuples) {
							double score = t.getScore();
							byte[] decompressedBytes = decompress(t.getBinaryElement());
							jedis.zadd(key, score, decompressedBytes);
						}
					}
					case "string" -> {
						byte[] val = jedis.get(key);
						byte[] decompressedBytes = decompress(val);
						jedis.set(key, decompressedBytes);
					}
			  }
			}
		} while (!new String(cursor).equals("0"));

	}

	public static byte[] decompress(byte[] compressedData) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);
		FramedLZ4CompressorOutputStream framedLZ4CompressorOutputStream = new FramedLZ4CompressorOutputStream(byteArrayOutputStream);
		byte[] buffer = new byte[512];
		int len;
		while ((len = byteArrayInputStream.read(buffer)) != -1) {
			framedLZ4CompressorOutputStream.write(buffer, 0, len);
		}

		return buffer;

	}

}
