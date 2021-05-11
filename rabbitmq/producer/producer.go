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
    notifyClose   chan *amqp.Error
    notifyConfirm chan amqp.Confirmation
    done          chan bool
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

// 连接conn and channel，根据bindingMode连接exchange and queue
func (p *Producer) connect() (err error) {
    p.logger.Println("attempt to connect rabbitmq")
    if p.conn, err = amqp.Dial(p.addr); err != nil {
        return err
    }
    if p.channel, err = p.conn.Channel(); err != nil {
        p.conn.Close()
        return err
    }
    p.channel.Confirm(false)

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

    p.logger.Println("rabbitmq is connected")

    return nil
}

// 自动创建(如果有则覆盖)exchange、queue，并绑定queue
func (p *Producer) activeBinding() (err error) {
    if err = p.channel.ExchangeDeclare(
        p.exchange,
        p.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        p.conn.Close()
        p.channel.Close()
        return err
    }

    if _, err = p.channel.QueueDeclare(
        p.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        p.conn.Close()
        p.channel.Close()
        return err
    }
    if err = p.channel.QueueBind(
        p.queue,
        p.routerKey,
        p.exchange,
        false,
        nil,
    ); err != nil {
        p.conn.Close()
        p.channel.Close()
        return err
    }

    return nil
}

// 检查exchange及queue是否存在，若不存在，則直接返回错误
// 存在则绑定exchange及queue
func (p *Producer) passiveBinding() (err error) {
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
        return err
    }
    if err = p.channel.QueueBind(
        p.queue,
        p.routerKey,
        p.exchange,
        false,
        nil,
    ); err != nil {
        p.conn.Close()
        p.channel.Close()
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

        }

        p.isConnected = false
        for !p.isConnected {
            if err := p.connect(); err != nil {
                p.logger.Println("failed to connect rabbitmq. Retrying...")
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
            p.logger.Println("push failed. retrying...")
            currentTimes += 1
            if currentTimes < resendTimes {
                time.Sleep(reconnectDelay)
                continue
            } else {
                return err
            }
        }

        ticker := time.NewTicker(resendDelay)
        select {
        case confirm := <-p.notifyConfirm:
            if confirm.Ack {
                p.logger.Println("push confirmed!")
                return nil
            }
        case <-ticker.C:
        }
        p.logger.Println("push didn't confirm. retrying...")
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
