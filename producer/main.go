package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic   = "wallet.topic.test.order.created"
	brokers = "182.16.4.66:9092"
)

func main() {
	producer, err := newProducer()
	if err != nil {
		log.Fatalf("❌ 创建生产者失败: %v", err)
	}
	defer producer.Close()
	wg := &sync.WaitGroup{}
	// 并发生产 5 个线程，每个线程不断发送消息
	st := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go produceMessages(producer, i, wg)
	}
	wg.Wait()
	et := time.Since(st)
	fmt.Println("use time=", et)
	//select {} // 阻塞主线程
}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner // 通过 Key 进行分区

	return sarama.NewSyncProducer([]string{brokers}, config)
}

func produceMessages(producer sarama.SyncProducer, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	count := 10
	for {
		if count == 0 {
			break
		}
		count--
		key := fmt.Sprintf("%d", id) // 3 个分区，Key 控制分区
		value := fmt.Sprintf("Producer %s: Msg %d", key, count)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key), // 相同 Key 进入同一个分区
			Value: sarama.StringEncoder(value),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("❌ 生产者 %d 发送消息失败: %v", id, err)
			continue
		}

		log.Printf("✅ 生产者 %d 发送消息: %s (Partition=%d, Offset=%d)", id, value, partition, offset)
		//time.Sleep(time.Millisecond * 500) // 控制发送速率
	}
}
