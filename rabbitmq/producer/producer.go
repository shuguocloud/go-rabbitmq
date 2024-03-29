package producer

import (
    "errors"
    "log"
    "os"
    "time"

    "github.com/streadway/amqp"
)

type Producer struct {
    connection      *amqp.Connection
    channel         *amqp.Channel
    notifyConnClose chan *amqp.Error       // 如果连接异常关闭，会接受数据
    notifyChanClose chan *amqp.Error       // 如果管道异常关闭，会接受数据
    notifyConfirm   chan amqp.Confirmation // 消息发送成功确认，会接受到数据
    done            chan bool              // 如果主动close，会接受数据
    isConnected     bool

    // product dial config
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

    // producer heartbeat
    heartbeat int64

    logger *log.Logger
}

const (
    // client default heartbeat
    defaultHeartbeat = 10 * time.Second

    // When reconnecting to the server after connection failure
    reconnectDelay = 5 * time.Second

    // When setting up the channel after a channel exception
    reInitDelay = 2 * time.Second

    // When resending messages the server didn't confirm
    resendDelay = 5 * time.Second
)

var (
    errNotConnected  = errors.New("not connected to a server")
    errAlreadyClosed = errors.New("already closed: not connected to the server")
    errShutdown      = errors.New("session is shutting down")
    errPushException = errors.New("already closed: push exceptionally")
)

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(addr, exchange, exchangeType, queue, routerKey string, bindingMode bool, headers amqp.Table, contentType string, heartbeat int64) *Producer {
    p := Producer{
        addr:         addr,
        exchange:     exchange,
        exchangeType: exchangeType,
        queue:        queue,
        routerKey:    routerKey,
        contentType:  contentType,
        bindingMode:  bindingMode,
        headers:      headers,
        heartbeat:    heartbeat,
        logger:       log.New(os.Stdout, "", log.LstdFlags),
        done:         make(chan bool),
    }

    go p.handleReconnect(addr)

    return &p
}

// 判断连接是否断开
func (p *Producer) IsClosed() bool {
    var isClosed bool
    if p.connection == nil {
        isClosed = true
        p.logger.Println("[go-rabbitmq] rabbitmq has closed!")
    }

    if p.connection != nil && p.connection.IsClosed() {
        isClosed = true
        p.logger.Println("[go-rabbitmq] rabbitmq has closed!")
    }
    return isClosed
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (p *Producer) handleReconnect(addr string) {
    for {
        p.isConnected = false
        p.logger.Println("[go-rabbitmq] Attempt to connect.")

        conn, err := p.connect(addr)
        if err != nil {
            p.logger.Println("[go-rabbitmq] Failed to connect. Retrying...")

            select {
            case <-p.done:
                return
            case <-time.After(reconnectDelay):
            }
            continue
        }

        if done := p.handleReInit(conn); done {
            break
        }
    }
}

func (p *Producer) setHeartBeat(heartbeat int64) time.Duration {
    if heartbeat == 0 {
        return defaultHeartbeat
    } else {
        return time.Duration(heartbeat) * time.Second
    }
}

// connect will create a new AMQP connection
func (p *Producer) connect(addr string) (*amqp.Connection, error) {
    conn, err := amqp.DialConfig(addr, amqp.Config{
        Heartbeat: p.setHeartBeat(p.heartbeat),
    })

    if err != nil {
        p.logger.Println("[go-rabbitmq] Failed to connect:", err.Error())
        return nil, err
    }

    //defer conn.Close()

    p.changeConnection(conn)
    p.logger.Println("[go-rabbitmq] Connected!")
    return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (p *Producer) handleReInit(conn *amqp.Connection) bool {
    for {
        p.isConnected = false

        err := p.init(conn)
        if err != nil {
            p.logger.Println("[go-rabbitmq] Failed to initialize channel. Retrying...")

            select {
            case <-p.done:
                return true
            case <-time.After(reInitDelay):
            }
            continue
        }

        select {
        case <-p.done:
            return true
        case err := <-p.notifyConnClose:
            if err != nil {
                p.logger.Printf("[go-rabbitmq] Connection closed [err=%s]. Reconnecting...", err.Error())
            }
            return false
        case err := <-p.notifyChanClose:
            if err != nil {
                p.logger.Printf("[go-rabbitmq] Channel closed [err=%s]. Re-running init...", err.Error())
            }
            return false
        }
    }
}

// init will initialize channel & declare queue
func (p *Producer) init(conn *amqp.Connection) error {
    ch, err := conn.Channel()
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to open a channel.", err.Error())
        if p.connection != nil {
            p.connection.Close()
        }
        return err
    }

    //defer ch.Close()

    err = ch.ExchangeDeclare(
        p.exchange,
        p.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to declare a exchange.", err.Error())
        if p.channel != nil {
            p.channel.Close()
        }
        if p.connection != nil {
            p.connection.Close()
        }
        return err
    }

    _, err = ch.QueueDeclare(
        p.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    )
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to declare a queue.", err.Error())
        if p.channel != nil {
            p.channel.Close()
        }
        if p.connection != nil {
            p.connection.Close()
        }
        return err
    }

    err = ch.QueueBind(
        p.queue,
        p.routerKey,
        p.exchange,
        false,
        nil,
    )
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to bind a queue.", err.Error())
        if p.channel != nil {
            p.channel.Close()
        }
        if p.connection != nil {
            p.connection.Close()
        }
        return err
    }

    err = ch.Confirm(false)
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to confirm a channel.", err.Error())
        if p.channel != nil {
            p.channel.Close()
        }
        if p.connection != nil {
            p.connection.Close()
        }
        return err
    }

    p.changeChannel(ch)
    p.isConnected = true
    p.logger.Println("[go-rabbitmq] Setup!")

    return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (p *Producer) changeConnection(connection *amqp.Connection) {
    p.connection = connection
    p.notifyConnClose = make(chan *amqp.Error)
    p.connection.NotifyClose(p.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (p *Producer) changeChannel(channel *amqp.Channel) {
    p.channel = channel
    p.notifyChanClose = make(chan *amqp.Error)
    p.notifyConfirm = make(chan amqp.Confirmation, 1)
    p.channel.NotifyClose(p.notifyChanClose)
    p.channel.NotifyPublish(p.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (p *Producer) Push(data []byte) error {
    if !p.isConnected {
        return errors.New("failed to push: not connected")
    }
    for {
        err := p.UnsafePush(data)
        if err != nil {
            p.logger.Printf("[go-rabbitmq] Push failed [err=%s]. Retrying...", err.Error())

            select {
            case <-p.done:
                return errShutdown
            case <-time.After(resendDelay):
            }
            continue
        }
        select {
        case confirm := <-p.notifyConfirm:
            if confirm.Ack {
                // p.logger.Println("[go-rabbitmq] Push confirmed delivery with delivery tag: %d", confirm.DeliveryTag)
                return nil
            }
        case <-time.After(resendDelay):
        }
        p.logger.Println("[go-rabbitmq] Push didn't confirm. Retrying...")

        // push消息异常处理
        if p.connection != nil && !p.connection.IsClosed() {
            p.logger.Println("[go-rabbitmq] Push exceptionally!")
            if p.channel != nil {
                p.channel.Close()
            }
            p.connection.Close()
            return errPushException
        }
    }
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (p *Producer) UnsafePush(data []byte) error {
    if !p.isConnected {
        return errNotConnected
    }
    return p.channel.Publish(
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
    )
}

// Close will cleanly shutdown the channel and connection.
func (p *Producer) Close() error {
    if !p.isConnected {
        return errAlreadyClosed
    }
    err := p.channel.Close()
    if err != nil {
        return err
    }
    err = p.connection.Close()
    if err != nil {
        return err
    }
    close(p.done)
    p.isConnected = false
    return nil
}
