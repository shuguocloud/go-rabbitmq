package consumer

import (
    "errors"
    "log"
    "os"
    "time"

    "github.com/streadway/amqp"
)

const (
    // every reconnect second when fail
    reconnectDelay = 5 * time.Second
)

var (
    errAlreadyClosed = errors.New("already closed: not connected to the consumer")
)

type Consumer struct {
    conn        *amqp.Connection
    channel     *amqp.Channel
    notifyClose chan *amqp.Error
    done        chan bool
    isConnected bool
    isConsume   bool

    addr         string
    consumerTag  string
    exchange     string
    exchangeType string
    queue        string
    routerKey    string
    // consumer handler
    handler func([]byte) error
    // true means auto create exchange and queue
    // false means passive create exchange and queue
    bindingMode bool

    logger *log.Logger
}

func New(addr, consumerTag, exchange, exchangeType, queue, routerKey string, handler func([]byte) error, bindingMode bool) *Consumer {
    consumer := Consumer{
        addr:         addr,
        consumerTag:  consumerTag,
        exchange:     exchange,
        exchangeType: exchangeType,
        queue:        queue,
        routerKey:    routerKey,
        handler:      handler,
        bindingMode:  bindingMode,
        logger:       log.New(os.Stdout, "", log.LstdFlags),
        done:         make(chan bool),
    }

    return &consumer
}

// 首次连接rabbitmq，有错即返回
// 沒有遇錯，则开启goroutine循环检查是否断线
func (c *Consumer) Start() error {
    if err := c.connect(); err != nil {
        return err
    }

    go c.reconnect()
    return nil
}

// 判断连接是否断开
func (c *Consumer) IsClosed() bool {
    c.logger.Println("[go-rabbitmq] rabbitmq has closed!")
    return c.conn.IsClosed()
}

// 连接conn and channel，根据bindingMode连接exchange and queue
func (c *Consumer) connect() (err error) {
    // 建立连接
    c.logger.Println("[go-rabbitmq] attempt to connect rabbitmq.")
    if c.conn, err = amqp.Dial(c.addr); err != nil {
        c.logger.Println("[go-rabbitmq] failed to connect to rabbitmq:", err.Error())
        return err
    }

    // 创建一个Channel
    if c.channel, err = c.conn.Channel(); err != nil {
        c.conn.Close()
        c.logger.Println("[go-rabbitmq] failed to open a channel:", err.Error())
        return err
    }

    if c.bindingMode {
        if err = c.activeBinding(); err != nil {
            return err
        }
    } else {
        if err = c.passiveBinding(); err != nil {
            return err
        }
    }

    c.isConnected = true
    c.notifyClose = make(chan *amqp.Error)
    c.channel.NotifyClose(c.notifyClose)
    c.logger.Println("[go-rabbitmq] rabbitmq is connected.")

    return nil
}

// 自动创建(如果有则覆盖)exchange、queue，并绑定queue
func (c *Consumer) activeBinding() (err error) {
    // 声明exchange
    if err = c.channel.ExchangeDeclare(
        c.exchange,
        c.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        c.conn.Close()
        c.channel.Close()
        c.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    // 声明一个queue
    if _, err = c.channel.QueueDeclare(
        c.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        c.conn.Close()
        c.channel.Close()
        c.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }

    // exchange 绑定 queue
    if err = c.channel.QueueBind(
        c.queue,
        c.routerKey,
        c.exchange,
        false,
        nil,
    ); err != nil {
        c.conn.Close()
        c.channel.Close()
        c.logger.Println("[go-rabbitmq] failed to bind a queue:", err.Error())
        return err
    }

    return nil
}

// 检查exchange及queue是否存在，若不存在，則直接返回错误
// 存在则绑定exchange及queue
func (c *Consumer) passiveBinding() (err error) {
    // 声明exchange
    if err = c.channel.ExchangeDeclarePassive(
        c.exchange,
        c.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        c.conn.Close()
        c.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    if _, err = c.channel.QueueDeclarePassive(
        c.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        c.conn.Close()
        c.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }
    if err = c.channel.QueueBind(
        c.queue,
        c.routerKey,
        c.exchange,
        false,
        nil,
    ); err != nil {
        c.conn.Close()
        c.channel.Close()
        c.logger.Println("[go-rabbitmq] failed to bind a queue:", err.Error())
        return err
    }

    return nil
}

// 重新连接rabbitmq，并且重新consume
// 如果连接rabbitmq失败，会一直重试直到成功
func (c *Consumer) reconnect() {
    for {
        select {
        case <-c.done:
            return
        case <-c.notifyClose:
            c.logger.Println("[go-rabbitmq] rabbitmq notify close!")
        }

        c.isConnected = false
        c.isConsume = false
        for {
            if !c.isConnected {
                if err := c.connect(); err != nil {
                    c.logger.Printf("[go-rabbitmq] failed to connect rabbitmq [err=%s]. Retrying...", err.Error())
                    time.Sleep(reconnectDelay)
                }
            }
            // 检查目前连线是成功的，并开启Consume
            // 避免连线马上断线的风险
            if c.isConnected && !c.isConsume {
                if err := c.Consume(); err != nil {
                    c.logger.Println("[go-rabbitmq] failed to consume rabbitmq. Retrying...")
                    time.Sleep(reconnectDelay)
                } else {
                    break
                }
            }
        }
    }
}

// 开启消費
func (c *Consumer) Consume() (err error) {
    c.logger.Println("[go-rabbitmq] attempt to consume rabbitmq.")
    var delivery <-chan amqp.Delivery
    if delivery, err = c.channel.Consume(
        c.queue,
        c.consumerTag,
        false,
        false,
        false,
        false,
        nil,
    ); err != nil {
        c.conn.Close()
        c.channel.Close()
        c.logger.Println("[go-rabbitmq] consume failed:", err.Error())
        return err
    }

    c.isConsume = true
    c.logger.Println("[go-rabbitmq] rabbitmq is consuming.")
    go c.handle(delivery)
    return nil
}

// handle data and ack
func (c *Consumer) handle(delivery <-chan amqp.Delivery) {
    for d := range delivery {
        if err := c.handler(d.Body); err == nil {
            c.logger.Println("[go-rabbitmq] consume success!")
            d.Ack(false)
        } else {
            c.logger.Println("[go-rabbitmq] some consume problem for data:", err.Error())
            d.Ack(false)
        }
    }
}

// 关闭连接及通道
func (c *Consumer) Close() error {
    if !c.isConnected {
        return errAlreadyClosed
    }
    err := c.channel.Close()
    if err != nil {
        return err
    }
    err = c.conn.Close()
    if err != nil {
        return err
    }
    close(c.done)
    c.isConnected = false
    c.isConsume = false
    return nil
}
