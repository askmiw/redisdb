# redisdb
像操作数据库一样操作redis
### 样例
```java
...前面初始化jedisPool
Jedis redis = jedisPool.getResource();
User user = ....需要保存的数据初始化
RedisDB db = new RedisDB(redis, "Test");
db.save(user); //保存

//查询 
List<User> list = db
	.has("name", "hello world") //查询姓名为 hello world的记录
	.execQuery(User.class);
for (User o : list) {
	System.out.println(o);
}
```
