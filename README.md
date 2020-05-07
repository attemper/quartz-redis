# quartz-redis

>A JobStore of [Quartz Scheduler](http://quartz-scheduler.org/) using Redis that supports data storage and distributed lock. [中文](./README_zh_CN.md)

**Used by**
>👉 Attemper: A distributed,multi-tenancy,job-flow scheduling application 👈
>>[Github](https://github.com/attemper/attemper)  
>>[Gitee](https://gitee.com/attemper/attemper)


## Features
- Support redis's distributed lock
- Support redis's cluster and sentinel mode

## Configurations
- Add dependency to `pom.xml`
``` xml
<dependency>
	<groupId>com.github.attemper</groupId>
	<artifactId>quartz-redis</artifactId>
	<version>0.9.2</version>
</dependency>
```

- Config `org.quartz.jobStore.xxx`
To use quartz-redis,you can config like the following properties in `quartz.properties`. 
If you want to use it in spring-boot apps, it's the same configurations in `.yml` or `.properties`

```
# job store class
org.quartz.jobStore.class = com.github.quartz.impl.redisjobstore.RedisJobStore

# redis host (optional)
org.quartz.jobStore.host = <default is localhost>

# redis password (optional)
org.quartz.jobStore.password = <default is null>

# redis port (optional)
org.quartz.jobStore.port = <default is 6379>

# redis database (optional)
org.quartz.jobStore.database = <default is 0>

# see https://lettuce.io/core/release/reference/index.html#redisuri.uri-syntax
org.quartz.jobStore.uri = <you can use it replace host/password/port/database,like redis://localhost>

# redis cluster mode
org.quartz.jobStore.clusterNodes = <using comma-delimited list uri>

# redis sentinel mode
org.quartz.jobStore.sentinelNodes = <using comma-delimited list uri>

# redis sentinel matser name
org.quartz.jobStore.sentinelMaster = <master node name>

# enable ssl or not (optional)
org.quartz.jobStore.ssl = <default is false>
```

- you should config one of
  - host,password,port and database
  - uri
  - clusterNodes
  - sentinelNodes and sentinelMaster

## Dependencies
`quartz-redis` depends on the flowing project
- [`quartz-core@com.github.attemper`](https://github.com/attemper/quartz)  
In the quartz,I add some functions to satisfy my requirements  
However, you can also use [Quartz Scheduler](http://quartz-scheduler.org/)  
- [`lettuce-core@io.lettuce`](https://github.com/lettuce-io/lettuce-core)  
A redis client which used by spring-boot2.x  
- `jackson-databind@com.fasterxml.jackson.core`  
Serialize and deserialize job/trigger/calendar via it

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