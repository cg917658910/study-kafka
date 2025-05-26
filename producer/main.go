package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

const (
	topic   = "wallet.topic.test.order.notify"
	brokers = "localhost:9092"
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
	for i := range 100 {
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

type OrderNotifyMessage struct {
	Data struct {
		Info      map[string]any `json:"info"`
		NotifyUrl string         `json:"notify_url"`
		OrderType string         `json:"order_type"`
		DataId    string         `json:"data_id"`
	} `json:"data"`
	MsgId    string `json:"msg_id"`
	Platform string `json:"platform"`
}

func produceMessages(producer sarama.SyncProducer, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	count := 300
	urls := []string{
		/* "http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify", */
		/* "http://localhost:8080/notify",
		"http://localhost:8080/notify",
		"http://localhost:8080/notify", */
		//"http://localhost:8081/notify", // 500
		//"http://localhost:8081/notify",           // 500
		//"http://localhost:8080/notify_not_found", // notfound */
		//"http://localhost:8080/notify_not_found", //notfound
		"http://localhost:8080/notify", //notfound
		//"",
	}
	fmt.Println("Send message ...")
	for {
		if count == 0 {
			break
		}
		// 随机选择一个 URL
		randIndex, _ := rand.Int(rand.Reader, big.NewInt(int64(len(urls))))

		url := urls[randIndex.Int64()]
		platforms := []string{
			"test", /* "cg", "notfound", "", */
		}
		randIndexPlatform, _ := rand.Int(rand.Reader, big.NewInt(int64(len(platforms))))
		platform := platforms[randIndexPlatform.Int64()]
		count--
		//fmt.Println("url=", url)
		key := fmt.Sprintf("%d", id) // 3 个分区，Key 控制分区
		//value := fmt.Sprintf("Producer %s: Msg %d Time %s", key, count, time.Now().Format("2006-01-02 15:04:05"))
		data := OrderNotifyMessage{}
		data.Platform = platform
		dataId := time.Now().Format("2006-01-02 15:04:05")
		data.Data.Info = map[string]any{
			"order_id":   "123",
			"order_type": "test",
			"order_time": time.Now().Format("2006-01-02 15:04:05"),
		}
		data.Data.DataId = dataId
		data.Data.OrderType = "test"
		data.Data.NotifyUrl = url
		data.MsgId = fmt.Sprintf("%s_%s", data.Platform, uuid.NewString())
		dataByte, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("❌ JSON 序列化失败: %v", err)
		}
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key), // 相同 Key 进入同一个分区
			Value: sarama.ByteEncoder(dataByte),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Printf("❌ 生产者 %d 发送消息失败: %v", id, err)
			continue
		}

		//log.Printf("✅ 生产者 %d 发送消息: %s (Partition=%d, Offset=%d)", id, value, partition, offset)
		//time.Sleep(time.Millisecond * 500) // 控制发送速率
	}
}
