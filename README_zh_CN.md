# quartz-redis

使用Redis来作为[Quartz Scheduler](http://quartz-scheduler.org/)的分布式存储介质，并使用分布式锁来保证任务同一时刻不重复执行 [English](./README.md)

**使用的项目**
>👉 Attemper: 分布式多租户的支持流程编排的任务调度平台 👈
>>[Github](https://github.com/attemper/attemper)  
>>[Gitee](https://gitee.com/attemper/attemper)

## 特点
- 支持分布式锁
- 支持集群和哨兵模式

## 配置
- 在`pom.xml`添加依赖
``` xml
<dependency>
	<groupId>com.github.attemper</groupId>
	<artifactId>quartz-redis</artifactId>
	<version>0.9.2</version>
</dependency>
```

- 配置`org.quartz.jobStore.xxx`
使用quartz-redis，将下列配置配到`quartz.properties`文件中。若系统是spring-boot项目，也适用`.yml` 或 `.properties`

```
# job store class
org.quartz.jobStore.class = com.github.quartz.impl.redisjobstore.RedisJobStore

# redis host (optional)
org.quartz.jobStore.host = <默认值为localhost>

# redis password (optional)
org.quartz.jobStore.password = <默认值为null>

# redis port (optional)
org.quartz.jobStore.port = <默认值为6379>

# redis database (optional)
org.quartz.jobStore.database = <默认值为0>

# 参考https://lettuce.io/core/release/reference/index.html#redisuri.uri-syntax
org.quartz.jobStore.uri = <可替代掉host/password/port/database,比如配置值为redis://localhost>

# redis 集群模式
org.quartz.jobStore.clusterNodes = <用,分割的uri>

# redis 哨兵模式
org.quartz.jobStore.sentinelNodes = <用,分割的uri>

# redis 哨兵主节点
org.quartz.jobStore.sentinelMaster = <哨兵模式的主节点名称>

# 是否开启ssl认证 (optional)
org.quartz.jobStore.ssl = <默认值为false>
```

- 只需配置其一
  - host,password,port和database
  - uri
  - clusterNodes
  - sentinelNodes和sentinelMaster

## 依赖
`quartz-redis` 依赖下列项目
- [`quartz-core@com.github.attemper`](https://github.com/attemper/quartz)  
我Fork了quartz，为了满足我司业务需求，添加了相关功能  
也可以使用原生的[Quartz Scheduler](http://quartz-scheduler.org/)  
- [`lettuce-core@io.lettuce`](https://github.com/lettuce-io/lettuce-core)  
spring-boot2.x所使用的redis client
- `jackson-databind@com.fasterxml.jackson.core`  
用作序列化和反序列化job/trigger/calendar等

```xml
<dependency>
	<groupId>com.github.attemper</groupId>
	<artifactId>quartz-core</artifactId>
	<version>2.3.2.2</version>
</dependency>
<dependency>
	<groupId>io.lettuce</groupId>
	<artifactId>lettuce-core</artifactId>
	<version>5.x</version>
</dependency>
<dependency>
	<groupId>com.fasterxml.jackson.core</groupId>
	<artifactId>jackson-databind</artifactId>
	<version>2.x</version>
</dependency>
```