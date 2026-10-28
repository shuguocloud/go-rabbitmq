// Command main 演示 go-rabbitmq producer/consumer 的基础用法。
// 本文件属 vendor/github.com/shuguocloud/go-rabbitmq 独立 module 的可执行入口，
// 展示 WaitReady / Close / 信号处理 / goroutine 生命周期管理 等完整链路，
// 与 Audit 系列（030/033/038/039/041 等）修复后的 API 对齐。
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/shuguocloud/go-rabbitmq/rabbitmq/consumer"
	"github.com/shuguocloud/go-rabbitmq/rabbitmq/producer"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	addr         = "amqp://guest:guest@localhost:5672/"
	exchange     = "amq.direct"
	exchangeType = "direct"
	queue        = "test.platform.mq"
	routerKey    = "test.platform.amq"
	contentType  = "application/json"
	heartbeat    = 60
)

func main() {
	headers := amqp.Table{"test.topic": "platformData"}

	// Audit 038 接线：bindingMode=true 主动声明 exchange/queue（示例开箱即跑）；
	// =false 走 passive 分支要求 broker 预建（生产推荐）。
	p := producer.New(addr, exchange, exchangeType, queue, routerKey, true, headers, contentType, heartbeat)
	if p == nil {
		log.Fatalf("[main] producer.New 返回 nil，配置非法，已禁用")
	}
	defer p.Close()

	c := consumer.New(addr, "simple-consumer", exchange, exchangeType, queue, routerKey,
		func(data []byte) error {
			log.Printf("[consumer] 收到 %d 字节: %s", len(data), string(data))
			return nil
		}, true, heartbeat)
	if c == nil {
		log.Fatalf("[main] consumer.New 返回 nil，配置非法，已禁用")
	}
	defer c.Close()

	// Audit 039：WaitReady 让生产 goroutine 不在建连窗口内立即失败。
	if ok := p.WaitReady(10 * time.Second); !ok {
		log.Fatalf("[main] producer 建连超时")
	}

	log.Printf("[main] 连接就绪，启动生产/消费 goroutine...")

	var wg sync.WaitGroup
	wg.Add(2)

	// 生产 goroutine：每秒发一条，Usable 判空后重建
	go func() {
		defer wg.Done()
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()

		for range tick.C {
			if !p.Usable() {
				log.Printf("[producer] 实例不可用，重建...")
				if err := p.Close(); err != nil && !producer.IsAlreadyClosed(err) {
					log.Printf("[producer] Close err: %v", err)
				}
				p = producer.New(addr, exchange, exchangeType, queue, routerKey, true, headers, contentType, heartbeat)
				if p == nil {
					log.Printf("[producer] 重建返回 nil，退出")
					return
				}
				p.WaitReady(10 * time.Second)
			}

			if err := p.Push([]byte("hello")); err != nil {
				log.Printf("[producer] Push err: %v", err)
			} else {
				log.Printf("[producer] Push 成功")
			}
		}
	}()

	// 消费 goroutine
	go func() {
		defer wg.Done()
		if err := c.Start(); err != nil {
			log.Printf("[consumer] Start err: %v", err)
			return
		}
		if err := c.Consume(); err != nil {
			log.Printf("[consumer] Consume err: %v", err)
		}
	}()

	// 信号处理：优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[main] 收到信号 %s，关闭并等待 goroutine 退出...", sig)

	// Close 幂等；总上限避免 Close 阻塞拖住进程
	done := make(chan struct{})
	go func() {
		p.Close()
		c.Close()
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("[main] 全部 goroutine 已退出")
	case <-time.After(3 * time.Second):
		log.Printf("[main] 关闭超时，强制退出")
	}
	fmt.Println("[main] bye")
}
