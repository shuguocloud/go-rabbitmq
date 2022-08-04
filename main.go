package main

import (
    "log"
    "time"

    "github.com/shuguocloud/go-rabbitmq/rabbitmq/consumer"
    "github.com/shuguocloud/go-rabbitmq/rabbitmq/producer"
    "github.com/streadway/amqp"
)

func main() {

    headers := make(amqp.Table)
    headers["test.topic"] = "platformData"

    go func() {
        var p *producer.Producer
        if p == nil || p != nil && p.IsClosed() {
            // 重新连接
            p = producer.New(
                "amqp://guest:guest@localhost:5672/",
                "amq.direct",
                "direct",
                "test.platform.mq",
                "test.platform.amq",
                false,
                headers,
                "application/json",
                60,
            )
        }

        for {
            // msg := bytes.NewBufferString("hello")
            //msg := bytes.NewBuffer([]byte("hello"))
            if err := p.Push([]byte("hello")); err != nil {
                // error handle
            }
            // 方便看log
            time.Sleep(1 * time.Second)
        }
    }()

    go func() {
        handler := func(data []byte) error {
            return nil
        }

        c := consumer.New(
            "amqp://guest:guest@localhost:5672/",
            "simple-consumer",
            "amq.direct",
            "direct",
            "test.platform.mq",
            "test.platform.amq",
            handler,
            false,
            60,
        )
        if err := c.Start(); err != nil {
            log.Panic(err)
        }

        if err := c.Consume(); err != nil {
            log.Panic(err)
        }
    }()

    select {}
}
