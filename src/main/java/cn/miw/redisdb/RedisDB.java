package cn.miw.redisdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;

/**
 * 像操作数据库一样操作Redis 
 * RedisDB
 * 
 * @author mrzhou@miw.cn
 * @since 2018-08-29 10:00
 */
public class RedisDB {

	private Jedis redis;
	private String dbName;
	private Map<String, String> condistions = new HashMap<String, String>();

	/**
	 * 实例化一个RedisDB
	 * 
	 * @param redis  Jedis实例
	 * @param dbName DbName不能为空串,不能为null
	 */
	public RedisDB(Jedis redis, String dbName) {
		this.dbName = dbName;
		this.redis = redis;
	}

	/**
	 * 保存任意对象
	 * 
	 * @author mrzhou@miw.cn
	 * @param entity 需要保存的实体
	 * @param        <T> 泛型支持
	 */
	public <T> void save(T entity) {
		String tableName = dbName + ":" + lowerClassName(entity.getClass().getSimpleName());
		JSONObject obj = JSONObject.parseObject(JSONObject.toJSONString(entity));
		int id = JSONObject.toJSON(entity).hashCode();
		save(obj, tableName, id + "", null);
	}

	private void save(JSONObject source, String tableName, String id, String seq) {
		redis.set(tableName + ":_source:" + id + (seq != null ? ":" + seq : ""), JSONObject.toJSONString(source));
		for (String key : source.keySet()) {
			Object value = source.get(key);
			if (value instanceof JSONArray) {
				JSONArray A = (JSONArray) value;
				int i = 0;
				for (Object x : A) {
					if (x instanceof JSONObject || x instanceof JSONArray) {
						save((JSONObject) x, tableName + ":" + key, id, (i++) + "");
					} else {
						String v = x instanceof Number ? getNumStr((Number) x) : (x.toString());
						redis.sadd(tableName + ":" + key + ":" + v, id);
					}
				}
			} else if (value instanceof JSONObject) {
				save((JSONObject) value, tableName + ":" + key, id, null);
			} else {
				if (value != null) {
					String v = value instanceof Number ? getNumStr((Number) value) : (value.toString());
					redis.sadd(tableName + ":" + key + ":" + v, id);
				}
			}
		}
	}

	private String getNumStr(Number num) {
		return num.longValue() != num.doubleValue() ? num.toString() : "" + num.longValue();
	}

	/**
	 * 清除查询条件
	 * 
	 * @author mrzhou@miw.cn
	 * @return this
	 */
	public RedisDB clear() {
		condistions.clear();
		return this;
	}

	/**
	 * 添加一个查询条件
	 * 
	 * @author mrzhou@miw.cn
	 * @param key   查询的key
	 * @param value 查询的key包含的值
	 * @return this
	 */
	public RedisDB has(String key, String value) {
		condistions.put(key, value);
		return this;
	}

	private String lowerClassName(String className) {
		char[] chars = className.toCharArray();
		if (chars[0] <= 'Z' && chars[0] >= 'A') {
			chars[0] += 32;
		}
		return new String(chars);
	}

	/**
	 * 执行查询并返回结果集,如果有条件返回的是交集
	 * 
	 * @author mrzhou@miw.cn
	 * @param clz 需要查询的实体类型
	 * @param     <T> 泛型支持
	 * @return 查询结果
	 */
	public <T> List<T> execQuery(Class<T> clz) {
		String table = dbName + ":" + lowerClassName(clz.getSimpleName());
		Set<String> allkeys = new HashSet<>();
		for (String k : condistions.keySet()) {
			String x = condistions.get(k);
			String key = table + ":" + k + ":" + x + "*";
			Set<String> keys = redis.keys(key);
			allkeys.addAll(keys);
		}
		String[] sinters = allkeys.toArray(new String[allkeys.size()]);
		System.out.println("sinters:::" + Arrays.toString(sinters));
		Set<String> members;
		System.out.println("查询条件数量:" + sinters.length);
		System.out.println("查询方式:" + (sinters.length > 1 ? "sinter" : (sinters.length == 1 ? "smembers" : "_source")));
		members = sinters.length > 1 ? redis.sinter(sinters)
				: sinters.length == 1 ? redis.smembers(sinters[0]) : redis.keys(table + ":_source:*");
		List<String> result = new ArrayList<>();
		members.forEach(k -> result.add(k.replace(table + ":_source:", "")));
		return _find(result, clz);
	}

	private <T> List<T> _find(List<String> list, Class<T> clz) {
		String table = dbName + ":" + lowerClassName(clz.getSimpleName()) + ":_source";
		List<T> aList = new ArrayList<>();
		for (String key : list) {
			String source = redis.get(table + ":" + key);
			T x = JSONObject.parseObject(source, clz);
			if (x != null) {
				aList.add((T) x);
			}
		}
		return aList;
	}
}
