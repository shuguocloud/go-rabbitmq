package main

import (
    "bytes"
    "log"
    "time"

    "github.com/shuguocloud/rabbitmq-go/rabbitmq/consumer"
    "github.com/shuguocloud/rabbitmq-go/rabbitmq/producer"
    "github.com/streadway/amqp"
)

func main() {

	headers := make(amqp.Table)
	headers["test.topic"] = "platformData"

    go func() {


        p := producer.New(
            "amqp://guest:guest@localhost:5672/",
            "amq.direct",
            "direct",
            "test.platform.mq",
            "test.platform.amq",
            false,
            headers,
        )
        if err := p.Start(); err != nil {
            log.Panic(err)
        }
        for {
            // msg := bytes.NewBufferString("hello")
            msg := bytes.NewBuffer([]byte("hello"))
            if err := p.Push(msg); err != nil {
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
