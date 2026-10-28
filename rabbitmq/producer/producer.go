package producer

import (
    "context"
    "errors"
    "log"
    "os"
    "sync"
    "sync/atomic"
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

    // Audit 030：串行化"发布 + 等确认"，并按 DeliveryTag 精确匹配本次发布，
    // 避免超时重发遗留的陈旧确认被误判为本次发送成功。
    pushMu sync.Mutex
    tagGen uint64 // 当前 channel 上已发布的消息序号（等价于 broker 侧 delivery tag）

    // Audit 031：connection/channel/isConnected/notify* 由重连 goroutine 与业务
    // goroutine 并发读写，统一由 stateMu 保护。锁序固定为 pushMu → stateMu。
    stateMu sync.RWMutex

    // Audit 039：建连就绪信号。未连接时 readyCh 非 nil，init 成功后关闭并置 nil；
    // 断线时重新创建，供 WaitReady/Push 在建连窗口内等待而不是直接失败丢消息。
    readyCh chan struct{}

    // Audit 033：生命周期状态机。closing 保证 Close 幂等且重连循环能感知退出；
    // doneOnce 保护 close(done)；wg 跟踪重连 goroutine，Close 时等待其退出。
    closing   int32
    doneOnce  sync.Once
    reconnect sync.WaitGroup

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

    // push times when fail
    resendTimes = 3

    // Audit 030：确认通道缓冲。streadway 的 confirms 在连接 reader goroutine 中
    // 向 listener 同步投递，缓冲过小（原为 1）会阻塞整个连接的帧分发。
    confirmBufferSize = 256

    // Audit 033：Close 等待重连 goroutine 退出的上限（Dial 最坏 30s，不能拖住进程退出）
    closeWaitTimeout = 5 * time.Second

    // Audit 039：Push 在建连/重连窗口内等待就绪的上限（覆盖启动首连与 reconnectDelay 抖动）
    pushReadyTimeout = 10 * time.Second

    // Audit 046：NotifyClose 通道缓冲。streadway 的 Connection.shutdown / Channel.shutdown
    // 持有连接（通道）锁同步投递通知，无缓冲时若此刻无接收者（如 init 执行期间）
    // 会阻塞投递，进而让 init 错误分支里的 Close() 卡在 destructor.Do 上，重连协程死锁。
    notifyBufferSize = 1
)

var (
    errNotConnected  = errors.New("not connected to a server")
    errAlreadyClosed = errors.New("already closed: not connected to the server")
    errPushException = errors.New("already closed: push exceptionally")
)

// IsAlreadyClosed 报告 err 是否表示"该 producer 已被关闭过"。
// Audit 051：Close 幂等（Audit 033），重复调用返回 errAlreadyClosed 属正常语义，
// 调用方（如 pkg/mq 的懒重建与退出钩子）据此区分"重复关闭"与真实关闭失败，
// 避免幂等路径刷 Error 日志误导排障。
func IsAlreadyClosed(err error) bool {
    return errors.Is(err, errAlreadyClosed)
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
// Audit 052：必填参数非法时返回 nil 且不启动重连协程，调用方需按 nil 判定配置错误。
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
        readyCh:      make(chan struct{}),
    }

    if err := p.validateConfig(); err != nil {
        p.logger.Printf("[go-rabbitmq] Invalid producer config [err=%s], producer disabled.", err.Error())
        return nil
    }

    p.reconnect.Add(1)
    go func() {
        defer p.reconnect.Done()
        p.handleReconnect(addr)
    }()

    return &p
}

// validateConfig 校验建连与声明/绑定所需的必填参数（Audit 052）。
// 这些字段为空时 init 必然失败，producer 会陷入 reconnectDelay/reInitDelay 无意义重拨，
// 既刷日志又让每次 Push 白等 pushReadyTimeout，故在构造期直接拒绝。
func (p *Producer) validateConfig() error {
    switch {
    case p.addr == "":
        return errors.New("addr is empty")
    case p.exchange == "":
        return errors.New("exchange is empty")
    case p.exchangeType == "":
        return errors.New("exchangeType is empty")
    case p.queue == "":
        return errors.New("queue is empty")
    case p.routerKey == "":
        return errors.New("routerKey is empty")
    }
    return nil
}

// 判断连接是否断开
func (p *Producer) IsClosed() bool {
    conn := p.currentConnection()
    if conn == nil {
        return true
    }
    return conn.IsClosed()
}

// setConnected / connected / currentConnection / currentChannel / connCloseChan /
// chanCloseChan / confirmChan 统一封装状态字段的并发安全访问（Audit 031）。
//
// Audit 039：状态切换同时驱动就绪信号——转为已连接时关闭 readyCh 唤醒等待者，
// 转为未连接时重建 readyCh，供后续 WaitReady 等待下一次建连。
func (p *Producer) setConnected(v bool) {
    p.stateMu.Lock()
    if v {
        p.isConnected = true
        if p.readyCh != nil {
            close(p.readyCh)
            p.readyCh = nil
        }
    } else {
        p.isConnected = false
        if p.readyCh == nil {
            p.readyCh = make(chan struct{})
        }
    }
    p.stateMu.Unlock()
}

// WaitReady 等待连接就绪，最长 timeout；就绪返回 true，期间 Close 或超时返回 false。
func (p *Producer) WaitReady(timeout time.Duration) bool {
    return p.waitReady(context.Background(), timeout)
}

// waitReady 与 WaitReady 相同，额外响应 ctx 取消（Audit 047）。
func (p *Producer) waitReady(ctx context.Context, timeout time.Duration) bool {
    p.stateMu.RLock()
    if p.isConnected {
        p.stateMu.RUnlock()
        return true
    }
    ready := p.readyCh
    p.stateMu.RUnlock()

    if ready == nil {
        return p.connected()
    }

    timer := time.NewTimer(timeout)
    defer timer.Stop()

    select {
    case <-ready:
        return true
    case <-p.done:
        return false
    case <-ctx.Done():
        return false
    case <-timer.C:
        return false
    }
}

// Usable 报告 producer 是否仍可复用：连接存活，或仍在建连/重连且未被 Close。
// 与 IsClosed 的区别是"conn 尚未登记"（首次建连/重连的 Dial 窗口）不算失效，
// 避免业务侧把正在建连的实例误判为断开而反复重建（Audit 039）。
func (p *Producer) Usable() bool {
    if p.isClosing() {
        return false
    }
    if p.connected() {
        return true
    }
    conn := p.currentConnection()
    return conn == nil || !conn.IsClosed()
}

func (p *Producer) connected() bool {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.isConnected
}

func (p *Producer) currentConnection() *amqp.Connection {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.connection
}

func (p *Producer) currentChannel() *amqp.Channel {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.channel
}

func (p *Producer) connCloseChan() chan *amqp.Error {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.notifyConnClose
}

func (p *Producer) chanCloseChan() chan *amqp.Error {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.notifyChanClose
}

func (p *Producer) confirmChan() chan amqp.Confirmation {
    p.stateMu.RLock()
    defer p.stateMu.RUnlock()
    return p.notifyConfirm
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (p *Producer) handleReconnect(addr string) {
    for {
        // Audit 033：Close 已开始时不再建连，避免留下无主连接
        if p.isClosing() {
            return
        }

        p.setConnected(false)

        // Audit 032：重拨前必须清理上一轮连接。channel 级异常（queue 被删、
        // 权限变更、PRECONDITION_FAILED 等）时连接本身可能仍存活，
        // 不关闭会随每次 channel 错误泄漏一条 TCP 连接。
        p.closeStaleConnection()

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
            // Audit 033：因关闭流程退出时，本次新建的连接必须回收，
            // 否则 Close 之后仍会留下一条无人管理的连接。
            conn.Close()
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
        if p.isClosing() {
            return true
        }

        p.setConnected(false)

        err := p.init(conn)
        if err != nil {
            // Audit 051：init 失败原因与该连接是否已关闭是两回事，分开记录，
            // 避免把 init 错误当成"连接关闭原因"误导排障。
            p.logger.Printf("[go-rabbitmq] Failed to initialize channel [err=%s]. Retrying...", err.Error())

            select {
            case <-p.done:
                return true
            case e := <-p.connCloseChan():
                if e != nil {
                    p.logger.Printf("[go-rabbitmq] Connection closed [err=%s]. Reconnecting...", e.Error())
                }
                return false
            case <-time.After(reInitDelay):
            }
            continue
        }

        select {
        case <-p.done:
            return true
        case err := <-p.connCloseChan():
            if err != nil {
                p.logger.Printf("[go-rabbitmq] Connection closed [err=%s]. Reconnecting...", err.Error())
            }
            return false
        case err := <-p.chanCloseChan():
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
        // Audit 046：此处不关闭 conn，交给 handleReconnect 的 closeStaleConnection 回收。
        return err
    }

    err = ch.Confirm(false)
    if err != nil {
        p.logger.Printf("[go-rabbitmq] Failed [err=%s] to confirm a channel.", err.Error())
        ch.Close()
        // Audit 046：不在此关闭 conn（连接已断时 Close 会阻塞在 streadway 的
        // destructor.Do 上等待 reader 的 shutdown 完成），交由外层重连循环回收。
        return err
    }

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
        ch.Close()
        // Audit 046：不在此关闭 conn（连接已断时 Close 会阻塞在 streadway 的
        // destructor.Do 上等待 reader 的 shutdown 完成），交由外层重连循环回收。
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
        ch.Close()
        // Audit 046：不在此关闭 conn（连接已断时 Close 会阻塞在 streadway 的
        // destructor.Do 上等待 reader 的 shutdown 完成），交由外层重连循环回收。
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
        ch.Close()
        // Audit 046：不在此关闭 conn（连接已断时 Close 会阻塞在 streadway 的
        // destructor.Do 上等待 reader 的 shutdown 完成），交由外层重连循环回收。
        return err
    }

    p.changeChannel(ch)
    p.setConnected(true)
    p.logger.Println("[go-rabbitmq] Setup!")

    return nil
}

// closeStaleConnection 关闭当前登记的 connection（stateMu 保护，nil 安全）。
func (p *Producer) closeStaleConnection() {
    if conn := p.currentConnection(); conn != nil {
        conn.Close()
    }
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (p *Producer) changeConnection(connection *amqp.Connection) {
    // Audit 046：带缓冲，保证 streadway 的 shutdown 同步投递不会因无接收者而阻塞。
    notifyConnClose := make(chan *amqp.Error, notifyBufferSize)

    p.stateMu.Lock()
    p.connection = connection
    p.notifyConnClose = notifyConnClose
    p.stateMu.Unlock()

    connection.NotifyClose(notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (p *Producer) changeChannel(channel *amqp.Channel) {
    p.pushMu.Lock()
    defer p.pushMu.Unlock()

    // Audit 046：带缓冲，保证 Channel.shutdown 同步投递不会卡住 connection reader。
    notifyChanClose := make(chan *amqp.Error, notifyBufferSize)
    notifyConfirm := make(chan amqp.Confirmation, confirmBufferSize)

    p.stateMu.Lock()
    p.tagGen = 0 // 新 channel 的 delivery tag 从 1 重新计数
    p.channel = channel
    p.notifyChanClose = notifyChanClose
    p.notifyConfirm = notifyConfirm
    p.stateMu.Unlock()

    channel.NotifyClose(notifyChanClose)
    channel.NotifyPublish(notifyConfirm)
}

// Push will push data onto the queue, and wait for confirm.
// 若超时未确认则重发，最多 resendTimes 次；仅当收到与本次发布 DeliveryTag
// 匹配的 Ack 才视为成功，避免陈旧确认导致的"假成功丢消息"。
func (p *Producer) Push(data []byte) error {
    return p.PushWithContext(context.Background(), data)
}

// PushWithContext 与 Push 语义一致，但受 ctx 约束（Audit 047）：建连等待、
// 重试等待与确认等待都会在 ctx 结束时提前返回 ctx.Err()，避免 broker 故障时
// 把调用方（含 asynctask 队列满时的内联执行路径）阻塞到 RPC 超时。
func (p *Producer) PushWithContext(ctx context.Context, data []byte) error {
    if err := ctx.Err(); err != nil {
        return err
    }

    // Audit 039：New 为异步建连，启动/断线窗口内直接返回 errNotConnected 会丢消息；
    // 此处先等待就绪（上限 pushReadyTimeout），仍不就绪才失败。
    if !p.connected() && !p.waitReady(ctx, pushReadyTimeout) {
        if err := ctx.Err(); err != nil {
            return err
        }
        return errNotConnected
    }

    // 串行化：同一 channel 同时只允许一条 in-flight 消息，保证 tag 匹配语义成立
    p.pushMu.Lock()
    defer p.pushMu.Unlock()

    for attempt := 1; ; attempt++ {
        if !p.connected() {
            return errNotConnected
        }

        // 清掉上一次超时重发遗留的陈旧确认
        confirmCh := p.confirmChan()
        p.drainConfirms(confirmCh)

        tag, err := p.publish(data)
        if err != nil {
            p.logger.Printf("[go-rabbitmq] Push failed [err=%s]. Retrying...", err.Error())
            if attempt >= resendTimes {
                return err
            }
            if !sleepWithContext(ctx, reconnectDelay) {
                return ctx.Err()
            }
            continue
        }

        acked, ok := p.waitConfirm(ctx, confirmCh, tag, resendDelay)
        switch {
        case ok && acked:
            return nil
        case !ok:
            p.logger.Println("[go-rabbitmq] Push didn't confirm. Retrying...")
        default:
            p.logger.Println("[go-rabbitmq] Push nacked by broker. Retrying...")
        }

        if err := ctx.Err(); err != nil {
            return err
        }
        if attempt >= resendTimes {
            return errPushException
        }
    }
}

// sleepWithContext 等待 d，ctx 结束时返回 false（Audit 047）。
func sleepWithContext(ctx context.Context, d time.Duration) bool {
    timer := time.NewTimer(d)
    defer timer.Stop()

    select {
    case <-timer.C:
        return true
    case <-ctx.Done():
        return false
    }
}

// waitConfirm 等待与 tag 匹配的确认。返回 (是否 Ack, 是否收到确认)。
// 未匹配的确认（陈旧 tag）直接丢弃，不参与本次判定；ctx 结束立即返回（Audit 047）。
func (p *Producer) waitConfirm(ctx context.Context, confirmCh chan amqp.Confirmation, tag uint64, timeout time.Duration) (acked bool, got bool) {
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    for {
        select {
        case confirm, ok := <-confirmCh:
            if !ok {
                return false, false
            }
            if confirm.DeliveryTag != tag {
                continue
            }
            return confirm.Ack, true
        case <-timer.C:
            return false, false
        case <-p.done:
            return false, false
        case <-ctx.Done():
            return false, false
        }
    }
}

// drainConfirms 非阻塞排空确认通道，防止陈旧确认占用缓冲/阻塞连接分发。
func (p *Producer) drainConfirms(confirmCh chan amqp.Confirmation) {
    for {
        select {
        case _, ok := <-confirmCh:
            if !ok {
                return
            }
        default:
            return
        }
    }
}

// publish 在持 pushMu 的前提下发布并返回本次消息的 delivery tag。
func (p *Producer) publish(data []byte) (uint64, error) {
    ch := p.currentChannel()
    if ch == nil {
        return 0, errNotConnected
    }

    if err := ch.Publish(
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
        return 0, err
    }

    p.tagGen++
    return p.tagGen, nil
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (p *Producer) UnsafePush(data []byte) error {
    if !p.connected() {
        return errNotConnected
    }

    p.pushMu.Lock()
    defer p.pushMu.Unlock()

    _, err := p.publish(data)
    return err
}

// Close will cleanly shutdown the channel and connection.
// Audit 033：幂等（重复调用返回 errAlreadyClosed 而非 panic），并等待重连
// goroutine 退出，避免 Close 之后仍有连接被建立成"无主连接"。
func (p *Producer) Close() error {
    if !atomic.CompareAndSwapInt32(&p.closing, 0, 1) {
        return errAlreadyClosed
    }

    var err error
    if ch := p.currentChannel(); ch != nil {
        if cerr := ch.Close(); cerr != nil {
            err = cerr
        }
    }
    if conn := p.currentConnection(); conn != nil {
        if cerr := conn.Close(); cerr != nil && err == nil {
            err = cerr
        }
    }

    p.closeDone()
    p.setConnected(false)
    p.waitReconnectExit()

    return err
}

// isClosing 报告是否已进入关闭流程。
func (p *Producer) isClosing() bool {
    return atomic.LoadInt32(&p.closing) == 1
}

// closeDone 幂等关闭 done 通道（WaitGroup/Push 等据此退出）。
func (p *Producer) closeDone() {
    p.doneOnce.Do(func() {
        close(p.done)
    })
}

// waitReconnectExit 等待重连 goroutine 退出，最多 closeWaitTimeout。
func (p *Producer) waitReconnectExit() {
    exited := make(chan struct{})
    go func() {
        p.reconnect.Wait()
        close(exited)
    }()

    select {
    case <-exited:
    case <-time.After(closeWaitTimeout):
        p.logger.Println("[go-rabbitmq] reconnect goroutine did not exit before timeout.")
    }
}
