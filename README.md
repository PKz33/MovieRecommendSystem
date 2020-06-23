## MovieRecommendSystem  
**服务启动**  
启动`redis`：`redis-server redis.conf`  
启动`elasticsearch`：`bin/elasticsearch -d`  
启动`mongodb`：`bin/mongod -config ./data/mongodb.conf`  
启动`zookeeper`：`bin/zkServer.sh start`  
启动`kafka`：`bin/kafka-server-start.sh -daemon ./config/server.properties`  
启动`flume`：`bin/flume-ng agent -c ./conf/ -f ./conf/log-kafka.properties -n agent -Dflume.root.logger=INFO.console`
