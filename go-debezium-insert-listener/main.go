package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

type DebeziumEvent struct {
	Payload struct {
		Op    string                 `json:"op"`
		After map[string]interface{} `json:"after"`
	} `json:"payload"`
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "mysql_server.inventory.bas_notify"
	groupID := "go-insert-listener"

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_8_0_0

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("无法创建 consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("⚠️  退出中...")
		cancel()
	}()
	handler := &InsertHandler{}

	//handler := ConsumerGroupHandler{}

	wg := &sync.WaitGroup{}
	consumerNum := 1
	for i := 0; i < consumerNum; i++ { // 消费者数目
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				err := consumerGroup.Consume(ctx, []string{topic}, handler)
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

type InsertHandler struct{}

func (h *InsertHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *InsertHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *InsertHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event DebeziumEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("❌ 无法解析 JSON: %v", err)
			continue
		}

		if event.Payload.Op == "c" && event.Payload.After != nil {
			fmt.Println("✅ 检测到 INSERT 事件:")
			for k, v := range event.Payload.After {
				fmt.Printf("  %s: %v\n", k, v)
			}
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
