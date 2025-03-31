package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

const (
	consumerGroup = "payment-group"
	topic         = "cg-topic"
	brokers       = "localhost:9092"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_2_3_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin // 轮询分区
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{brokers}, consumerGroup, config)
	if err != nil {
		log.Fatalf("❌ 创建消费者组失败: %v", err)
	}
	defer group.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("⚠️  退出中...")
		cancel()
	}()

	handler := ConsumerGroupHandler{}

	wg := &sync.WaitGroup{}
	consumerNum := 1
	for i := 0; i < consumerNum; i++ { // 消费者数目
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				err := group.Consume(ctx, []string{topic}, &handler)
				if err != nil {
					log.Printf("❌ 消费者 %d 发生错误: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("✅ 消费者已全部退出")
}

// 消费者逻辑
type ConsumerGroupHandler struct{}

func (h ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("📩 分区 %d | 线程 %d 收到: %s\n", msg.Partition, os.Getpid(), string(msg.Value))
		session.MarkMessage(msg, "") // 标记已消费
	}
	return nil
}
