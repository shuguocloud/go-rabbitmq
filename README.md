# rabbitmq-go
封装rabbitmq client的library并实现producer and consumer机制

## 實現功能
+ 实现producer and consumer机制
+ 实现断线重来机制
+ 实现push失败，重新push机制
+ 实现consume失败(指的不是consume成功而业务逻辑消费数据失败的意思，而是可能因为连线失败、或是queue名称不对等原因)，重新consume机制

## How to use
+ 可参考main.go的使用方式
+ 基本上封装的方式不难，可实际看rabbitmq/producer.go與rabbitmq/consumer.go的程式码，并根据需求进行修正
+ 都有为详细注解

## Contribution
欢迎fork或issue，让封装的机制可以更弹性
