package producer

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
    // every push message second when fail
    resendDelay = 5 * time.Second
    // push times when fail
    resendTimes = 3
)

var (
    errNotConnected  = errors.New("not connected to the producer")
    errAlreadyClosed = errors.New("already closed: not connected to the producer")
)

type Producer struct {
    conn          *amqp.Connection
    channel       *amqp.Channel
    notifyClose   chan *amqp.Error       // 如果异常关闭，会接受数据
    notifyConfirm chan amqp.Confirmation // 消息发送成功确认，会接受到数据
    done          chan bool              // 如果主动close，会接受数据
    isConnected   bool

    addr         string
    exchange     string
    exchangeType string
    queue        string
    routerKey    string
    contentType  string
    // true means auto create exchange and queue
    // false means passive create exchange and queue
    bindingMode bool

    headers amqp.Table

    logger *log.Logger
}

func New(addr, exchange, exchangeType, queue, routerKey string, bindingMode bool, headers amqp.Table, contentType string) *Producer {
    producer := Producer{
        addr:         addr,
        exchange:     exchange,
        exchangeType: exchangeType,
        queue:        queue,
        routerKey:    routerKey,
        bindingMode:  bindingMode,
        headers:      headers,
        contentType:  contentType,
        logger:       log.New(os.Stdout, "", log.LstdFlags),
        done:         make(chan bool),
    }

    return &producer
}

// 首次连接rabbitmq，有错即返回
// 沒有遇错，则开启goroutine循环检查是否断线
func (p *Producer) Start() error {
    if err := p.connect(); err != nil {
        return err
    }

    go p.reconnect()
    return nil
}

// 判断连接是否断开
func (p *Producer) IsClosed() bool {
    var isClosed bool
    if p.conn.IsClosed() {
        isClosed = true
        p.logger.Println("[go-rabbitmq] rabbitmq has closed!")
    }
    return isClosed
}

// 连接conn and channel，根据bindingMode连接exchange and queue
func (p *Producer) connect() (err error) {
    // 建立连接
    p.logger.Println("[go-rabbitmq] attempt to connect rabbitmq.")
    if p.conn, err = amqp.Dial(p.addr); err != nil {
        p.logger.Println("[go-rabbitmq] failed to connect to rabbitmq:", err.Error())
        return err
    }

    // 创建一个Channel
    if p.channel, err = p.conn.Channel(); err != nil {
        p.logger.Println("[go-rabbitmq] failed to open a channel:", err.Error())
        p.conn.Close()
        return err
    }

    err = p.channel.Confirm(false)
    if err != nil {
        p.logger.Println("[go-rabbitmq] failed to confirm a channel:", err.Error())
        return err
    }

    if p.bindingMode {
        if err = p.activeBinding(); err != nil {
            return err
        }
    } else {
        if err = p.passiveBinding(); err != nil {
            return err
        }
    }

    p.isConnected = true
    p.notifyClose = make(chan *amqp.Error)
    p.notifyConfirm = make(chan amqp.Confirmation)
    p.channel.NotifyClose(p.notifyClose)
    p.channel.NotifyPublish(p.notifyConfirm)

    p.logger.Println("[go-rabbitmq] rabbitmq is connected.")

    return nil
}

// 自动创建(如果有则覆盖)exchange、queue，并绑定queue
func (p *Producer) activeBinding() (err error) {
    // 声明exchange
    if err = p.channel.ExchangeDeclare(
        p.exchange,
        p.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        p.channel.Close()
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    // 声明一个queue
    if _, err = p.channel.QueueDeclare(
        p.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        p.channel.Close()
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }

    // exchange 绑定 queue
    if err = p.channel.QueueBind(
        p.queue,
        p.routerKey,
        p.exchange,
        false,
        nil,
    ); err != nil {
        p.channel.Close()
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to bind a queue:", err.Error())
        return err
    }

    return nil
}

// 检查exchange及queue是否存在，若不存在，則直接返回错误
// 存在则绑定exchange及queue
func (p *Producer) passiveBinding() (err error) {
    // 声明exchange
    if err = p.channel.ExchangeDeclarePassive(
        p.exchange,
        p.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    if _, err = p.channel.QueueDeclarePassive(
        p.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }
    if err = p.channel.QueueBind(
        p.queue,
        p.routerKey,
        p.exchange,
        false,
        nil,
    ); err != nil {
        p.channel.Close()
        p.conn.Close()
        p.logger.Println("[go-rabbitmq] failed to bind a queue:", err.Error())
        return err
    }

    return nil
}

// 重新连接rabbitmq
func (p *Producer) reconnect() {
    for {
        select {
        case <-p.done:
            return
        case <-p.notifyClose:
            p.logger.Println("[go-rabbitmq] rabbitmq notify close!")
        }

        if p.conn != nil && !p.conn.IsClosed() {
            // IMPORTANT: 必须清空 Notify，否则死连接不会释放
            for err := range p.notifyClose {
                println(err)
            }

            p.channel.Close()
            p.conn.Close()
        }

        p.isConnected = false
        for !p.isConnected {
            if err := p.connect(); err != nil {
                p.logger.Printf("[go-rabbitmq] failed to connect rabbitmq [err=%s]. Retrying...", err.Error())
                time.Sleep(reconnectDelay)
            }
        }
    }
}

// 当push失败，则wait resend time，在重新push
// 累积resendTimes则返回error
// push成功，检查有无回传confirm，沒有也代表push失败，并wait resend time，在重新push
func (p *Producer) Push(data []byte) error {
    if !p.isConnected {
        p.logger.Println(errNotConnected.Error())
    }

    var currentTimes int
    for {
        if err := p.channel.Publish(
            p.exchange,  // Exchange
            p.routerKey, // Routing key
            false,       // Mandatory
            false,       // Immediate
            amqp.Publishing{
                Headers:         p.headers,
                ContentType:     p.contentType,
                ContentEncoding: "",
                Body:            data,
                DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
                Priority:        0,               // 0-9
                Timestamp:       time.Now(),
            },
        ); err != nil {
            p.logger.Printf("[go-rabbitmq] push failed [err=%s]. Retrying...", err.Error())
            currentTimes += 1
            if currentTimes < resendTimes {
                time.Sleep(reconnectDelay)
                continue
            } else {
                p.logger.Println("[go-rabbitmq] push failed:", err.Error())
                return err
            }
        }

        ticker := time.NewTicker(resendDelay)
        select {
        case confirm := <-p.notifyConfirm:
            if confirm.Ack {
                p.logger.Println("[go-rabbitmq] push confirmed delivery with delivery tag: %d", confirm.DeliveryTag)
                return nil
            }
        case <-ticker.C:
            p.logger.Println("[go-rabbitmq] push timeout!")
        }

        p.logger.Println("[go-rabbitmq] push didn't confirm. retrying...")

        // push消息异常处理
        if p.conn != nil && !p.conn.IsClosed() {
            p.channel.Close()
            p.conn.Close()
            p.isConnected = false
            p.logger.Println("[go-rabbitmq] push exceptionally!")
            return errAlreadyClosed
        }
    }
}

// 关闭rabbitmq conn and channel
func (p *Producer) Close() error {
    if !p.isConnected {
        return errAlreadyClosed
    }
    err := p.channel.Close()
    if err != nil {
        return err
    }
    err = p.conn.Close()
    if err != nil {
        return err
    }
    close(p.done)
    p.isConnected = false
    return nil
}
