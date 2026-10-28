package consumer

import (
    "errors"
    "fmt"
    "log"
    "os"
    "runtime/debug"
    "sync"
    "sync/atomic"
    "time"

    "github.com/streadway/amqp"
)

const (
    // client default heartbeat
    defaultHeartbeat = 10 * time.Second
    // every reconnect second when fail
    reconnectDelay = 5 * time.Second

    // Audit 036：默认预取条数。RabbitMQ 对 autoAck=false 的消费者默认
    // unlimited prefetch，消费慢于生产时消息会无界堆积在进程内直至 OOM。
    defaultPrefetchCount = 50

    // Audit 037：Close 等待消费协程退出的上限，避免拖住进程退出。
    handleWaitTimeout = 5 * time.Second
)

var (
    errAlreadyClosed = errors.New("already closed: consumer is closed")
    errNotConnected  = errors.New("not connected to the consumer")
)

// Option 定义可选配置项；New 保留原有参数列表并追加可变参数，向后兼容。
type Option func(*Consumer)

// WithPrefetch 设置 QoS prefetch count；count <= 0 时沿用默认值 defaultPrefetchCount。
func WithPrefetch(count int) Option {
    return func(c *Consumer) {
        if count > 0 {
            c.prefetch = count
        }
    }
}

// WithRequeueOnFailure 设置 handler 返回错误或 panic 时是否重新入队。
// 默认 true（消息不丢）；对不可重试的"毒消息"应置 false 并配合死信队列。
func WithRequeueOnFailure(requeue bool) Option {
    return func(c *Consumer) {
        c.requeueOnFailure = requeue
    }
}

// WithLogger 自定义日志输出（默认 stdout）。
func WithLogger(logger *log.Logger) Option {
    return func(c *Consumer) {
        if logger != nil {
            c.logger = logger
        }
    }
}

type Consumer struct {
    conn          *amqp.Connection
    channel       *amqp.Channel
    connNotify    chan *amqp.Error
    channelNotify chan *amqp.Error
    done          chan struct{}
    isConnected   bool
    isConsume     bool

    // consumer dial config
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

    // consumer heartbeat
    heartbeat int64

    // Audit 036：QoS 预取条数
    prefetch int

    // Audit 035：handler 失败/panic 时是否 requeue
    requeueOnFailure bool

    // Audit 037：生命周期。closing 保证 Close 幂等且重连循环能感知退出；
    // doneOnce 保护 close(done)；handleWg 跟踪消费协程，Close 时等待其退出。
    closing   int32
    doneOnce  sync.Once
    startOnce sync.Once
    startErr  error
    handleWg  sync.WaitGroup

    logger *log.Logger
}

func New(addr, consumerTag, exchange, exchangeType, queue, routerKey string, handler func([]byte) error, bindingMode bool, heartbeat int64, opts ...Option) *Consumer {
    consumer := &Consumer{
        addr:             addr,
        consumerTag:      consumerTag,
        exchange:         exchange,
        exchangeType:     exchangeType,
        queue:            queue,
        routerKey:        routerKey,
        handler:          handler,
        bindingMode:      bindingMode,
        heartbeat:        heartbeat,
        prefetch:         defaultPrefetchCount,
        requeueOnFailure: true,
        logger:           log.New(os.Stdout, "", log.LstdFlags),
        done:             make(chan struct{}),
    }

    for _, opt := range opts {
        opt(consumer)
    }

    return consumer
}

// 首次连接rabbitmq，有错即返回
// 沒有遇錯，则开启goroutine循环检查是否断线
func (c *Consumer) Start() error {
    // Audit 044：Start 幂等，重复调用不会叠加多个 reconnect 协程。
    c.startOnce.Do(func() {
        if c.isClosing() {
            c.startErr = errAlreadyClosed
            return
        }
        if err := c.connect(); err != nil {
            c.startErr = err
            return
        }
        go c.reconnect()
    })

    return c.startErr
}

// 判断连接是否断开
func (c *Consumer) IsClosed() bool {
    if c.conn == nil || c.conn.IsClosed() {
        c.logger.Println("[go-rabbitmq] rabbitmq has closed!")
        return true
    }
    return false
}

func (c *Consumer) setHeartBeat(heartbeat int64) time.Duration {
    if heartbeat == 0 {
        return defaultHeartbeat
    } else {
        return time.Duration(heartbeat) * time.Second
    }
}

// 连接conn and channel，根据bindingMode连接exchange and queue
// Audit 044：所有状态在全部步骤成功后一次性提交，避免失败时留下半初始化状态。
func (c *Consumer) connect() (err error) {
    // 建立连接
    c.logger.Println("[go-rabbitmq] attempt to connect rabbitmq.")

    conn, err := amqp.DialConfig(c.addr, amqp.Config{
        Heartbeat: c.setHeartBeat(c.heartbeat),
    })
    if err != nil {
        c.logger.Println("[go-rabbitmq] failed to connect to rabbitmq:", err.Error())
        return err
    }

    // Audit 044：建连期间收到 Close，回收本次新建连接，避免留下无主连接。
    if c.isClosing() {
        conn.Close()
        return errAlreadyClosed
    }

    // 创建一个Channel
    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        c.logger.Println("[go-rabbitmq] failed to open a channel:", err.Error())
        return err
    }

    if c.bindingMode {
        err = c.activeBinding(ch)
    } else {
        err = c.passiveBinding(ch)
    }
    if err != nil {
        ch.Close()
        conn.Close()
        return err
    }

    c.conn = conn
    c.channel = ch
    c.connNotify = conn.NotifyClose(make(chan *amqp.Error))
    c.channelNotify = ch.NotifyClose(make(chan *amqp.Error))
    c.isConnected = true
    c.logger.Println("[go-rabbitmq] rabbitmq is connected.")

    return nil
}

// 自动创建(如果有则覆盖)exchange、queue，并绑定queue
func (c *Consumer) activeBinding(ch *amqp.Channel) (err error) {
    // 声明exchange
    if err = ch.ExchangeDeclare(
        c.exchange,
        c.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        c.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    // 声明一个queue
    if _, err = ch.QueueDeclare(
        c.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        c.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }

    // exchange 绑定 queue
    if err = ch.QueueBind(
        c.queue,
        c.routerKey,
        c.exchange,
        false,
        nil,
    ); err != nil {
        c.logger.Println("[go-rabbitmq] failed to bind a queue:", err.Error())
        return err
    }

    return nil
}

// 检查exchange及queue是否存在，若不存在，則直接返回错误
// 存在则绑定exchange及queue
func (c *Consumer) passiveBinding(ch *amqp.Channel) (err error) {
    // 声明exchange
    if err = ch.ExchangeDeclarePassive(
        c.exchange,
        c.exchangeType,
        true,
        false,
        false,
        false,
        nil,
    ); err != nil {
        c.logger.Println("[go-rabbitmq] failed to declare a exchange:", err.Error())
        return err
    }

    if _, err = ch.QueueDeclarePassive(
        c.queue,
        true,  // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    ); err != nil {
        c.logger.Println("[go-rabbitmq] failed to declare a queue:", err.Error())
        return err
    }

    if err = ch.QueueBind(
        c.queue,
        c.routerKey,
        c.exchange,
        false,
        nil,
    ); err != nil {
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
        case err := <-c.connNotify:
            if err != nil {
                c.logger.Printf("[go-rabbitmq] rabbitmq consumer - connect notify close err=[%s]!", err.Error())
            }
        case err := <-c.channelNotify:
            if err != nil {
                c.logger.Printf("[go-rabbitmq] rabbitmq consumer - channel notify close err=[%s]!", err.Error())
            }
        }

        // Audit 037：Close 已开始则不再重连，避免"关掉又复活"。
        if c.isClosing() {
            return
        }

        if c.conn != nil && !c.conn.IsClosed() {
            // 关闭 SubMsg common delivery
            if err := c.channel.Cancel(c.consumerTag, true); err != nil {
                c.logger.Println("[go-rabbitmq] rabbitmq consumer - channel cancel failed: ", err.Error())
            }
            if err := c.channel.Close(); err != nil {
                c.logger.Println("[go-rabbitmq] rabbitmq consumer - channel close failed: ", err.Error())
            }
            if err := c.conn.Close(); err != nil {
                c.logger.Println("[go-rabbitmq] rabbitmq consumer - connection close failed: ", err.Error())
            }
        }

        // IMPORTANT: 必须清空 Notify，否则死连接不会释放
        // Audit 037：残留的内置 println 已改为 logger 并打印错误内容。
        for err := range c.channelNotify {
            if err != nil {
                c.logger.Printf("[go-rabbitmq] rabbitmq consumer - channel notify drained err=[%s].", err.Error())
            }
        }

        for err := range c.connNotify {
            if err != nil {
                c.logger.Printf("[go-rabbitmq] rabbitmq consumer - connect notify drained err=[%s].", err.Error())
            }
        }

        c.isConnected = false
        c.isConsume = false
        for {
            // Audit 037：内层重试必须感知 Close，否则 Close 之后消费者会重新上线消费。
            if c.isClosing() {
                return
            }

            if !c.isConnected {
                if err := c.connect(); err != nil {
                    c.logger.Printf("[go-rabbitmq] failed to connect rabbitmq [err=%s]. Retrying...", err.Error())
                    if !c.sleep(reconnectDelay) {
                        return
                    }
                }
            }
            // 检查目前连线是成功的，并开启Consume
            // 避免连线马上断线的风险
            if c.isConnected && !c.isConsume {
                if err := c.Consume(); err != nil {
                    c.logger.Printf("[go-rabbitmq] failed to consume rabbitmq [err=%s]. Retrying...", err.Error())
                    if !c.sleep(reconnectDelay) {
                        return
                    }
                } else {
                    break
                }
            }
        }
    }
}

// sleep 等待 d，期间收到 Close 则立即返回 false。
func (c *Consumer) sleep(d time.Duration) bool {
    timer := time.NewTimer(d)
    defer timer.Stop()

    select {
    case <-c.done:
        return false
    case <-timer.C:
        return true
    }
}

// 开启消費
func (c *Consumer) Consume() (err error) {
    c.logger.Printf("[go-rabbitmq] attempt to consume rabbitmq [queue=%s prefetch=%d].", c.queue, c.prefetch)

    if c.isClosing() {
        return errAlreadyClosed
    }
    if c.channel == nil {
        return errNotConnected
    }

    // Audit 036：开始消费前先设置 QoS，避免 broker 无界推送。
    if err = c.channel.Qos(c.prefetch, 0, false); err != nil {
        c.logger.Println("[go-rabbitmq] failed to set qos:", err.Error())
        c.dropConnection()
        return err
    }

    delivery, err := c.channel.Consume(
        c.queue,
        c.consumerTag,
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        c.logger.Println("[go-rabbitmq] consume failed:", err.Error())
        // Audit 044：失败后必须复位连接态并回收，否则 reconnect 内层会因
        // isConnected 仍为 true 而跳过 connect，陷入"反复 Consume 失败"死循环。
        c.dropConnection()
        return err
    }

    c.isConsume = true
    c.logger.Println("[go-rabbitmq] rabbitmq is consuming.")
    c.handleWg.Add(1)
    go c.handle(delivery)
    return nil
}

// dropConnection 标记断开并回收当前 conn/channel，交由 reconnect 重建。
func (c *Consumer) dropConnection() {
    c.isConnected = false
    c.isConsume = false

    if c.channel != nil {
        if err := c.channel.Close(); err != nil && err != amqp.ErrClosed {
            c.logger.Println("[go-rabbitmq] channel close failed: ", err.Error())
        }
    }
    if c.conn != nil {
        if err := c.conn.Close(); err != nil && err != amqp.ErrClosed {
            c.logger.Println("[go-rabbitmq] connection close failed: ", err.Error())
        }
    }
}

// handle data and ack
// Audit 035：handler 失败改 Nack（不再无条件 Ack 导致消息丢失），
// panic 由 callHandler 单条隔离，消费循环不会因单条消息崩溃而静默停止。
func (c *Consumer) handle(delivery <-chan amqp.Delivery) {
    defer c.handleWg.Done()

    for d := range delivery {
        c.dispatch(d)
    }

    c.logger.Println("[go-rabbitmq] delivery channel closed, consumer handler exited.")
}

// dispatch 处理单条消息并按结果 Ack/Nack，确认错误全部记录。
func (c *Consumer) dispatch(d amqp.Delivery) {
    if err := c.callHandler(d.Body); err != nil {
        c.logger.Printf("[go-rabbitmq] consume failed [tag=%d err=%s], nack with requeue=%v.", d.DeliveryTag, err.Error(), c.requeueOnFailure)
        if nerr := d.Nack(false, c.requeueOnFailure); nerr != nil {
            c.logger.Printf("[go-rabbitmq] nack failed [tag=%d err=%s].", d.DeliveryTag, nerr.Error())
        }
        return
    }

    if err := d.Ack(false); err != nil {
        c.logger.Printf("[go-rabbitmq] ack failed [tag=%d err=%s].", d.DeliveryTag, err.Error())
    }
}

// callHandler 调用业务 handler 并隔离 panic，panic 转为 error 走 Nack 流程。
func (c *Consumer) callHandler(body []byte) (err error) {
    defer func() {
        if r := recover(); r != nil {
            c.logger.Printf("[go-rabbitmq] handler panic recovered [panic=%v]\n%s", r, debug.Stack())
            err = fmt.Errorf("handler panic: %v", r)
        }
    }()

    return c.handler(body)
}

// 关闭连接及通道
// Audit 037：Close 幂等（重复调用返回 errAlreadyClosed 而非 panic），
// 且未连接时也可关闭（后台重连/消费协程仍需停止）。
func (c *Consumer) Close() error {
    if !atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
        return errAlreadyClosed
    }

    // 先关闭 done，通知 reconnect/handle/sleep 退出。
    c.closeDone()

    var err error
    if c.channel != nil {
        if cerr := c.channel.Close(); cerr != nil && cerr != amqp.ErrClosed {
            err = cerr
        }
    }
    if c.conn != nil {
        if cerr := c.conn.Close(); cerr != nil && cerr != amqp.ErrClosed && err == nil {
            err = cerr
        }
    }

    c.isConnected = false
    c.isConsume = false
    c.waitHandleExit()

    return err
}

// isClosing 报告是否已进入关闭流程。
func (c *Consumer) isClosing() bool {
    return atomic.LoadInt32(&c.closing) == 1
}

// closeDone 幂等关闭 done 通道。
func (c *Consumer) closeDone() {
    c.doneOnce.Do(func() {
        close(c.done)
    })
}

// waitHandleExit 等待消费协程退出，最多 handleWaitTimeout。
func (c *Consumer) waitHandleExit() {
    exited := make(chan struct{})
    go func() {
        c.handleWg.Wait()
        close(exited)
    }()

    select {
    case <-exited:
    case <-time.After(handleWaitTimeout):
        c.logger.Println("[go-rabbitmq] consumer handler did not exit before timeout.")
    }
}
