# rocketmq_trans_message
基于rocketmq上加入了基于数据库的事务消息的功能.官方的rocketmq阉割了此功能.

# 使用方法:
# 1.rocketmq配置文件中加入以下几个配置:
```
jdbcURL=jdbc:mysql://xxxxx/xxxxx?useUnicode=true&characterEncoding=utf8&noAccessToProcedureBodies=true
jdbcUser=xxxxx
jdbcPassword=xxxxxx
```

# 2.打包
mvn -Dmaven.test.skip=true -Dcheckstyle.skip=true clean package install assembly:assembly -P release-all -U

# 3.example
TransactionProducer 事务发送者
TransactionConsumer 事务消费者


***如果大家有任何的问题可以加我的QQ:309749236,或者直接issues,我看见了会给你们解答***

