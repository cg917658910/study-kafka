package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/cg917658910/study-kafka/consumer/tracker"
)

const (
	consumerGroup = "wallet.group.test2.order.notify"
	topic         = "wallet.topic.test.order.notify"
	brokers       = "localhost:9092"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_2_3_0

	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin() // 轮询分区
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false //关闭自动提交
	config.Consumer.Return.Errors = true
	/* config.Consumer.Group.Session.Timeout = 10 * 60 * 1000   // 10分钟
	config.Consumer.Group.Heartbeat.Interval = 3 * 60 * 1000 // 3分钟
	config.Consumer.Group.Rebalance.Timeout = 10 * 60 * 1000 // 10分钟
	config.Consumer.Group.Rebalance.Retry.Max = 10
	config.Consumer.Group.Rebalance.Retry.Backoff = 10 * 1000 // 10秒 */

	group, err := sarama.NewConsumerGroup([]string{brokers}, consumerGroup, config)
	if err != nil {
		log.Fatalf("❌ 创建消费者组失败: %v", err)
	}
	defer group.Close()

	// 监听 Kafka 消费者组错误
	go func() {
		for err := range group.Errors() {
			log.Printf("⚠️ 消费者组错误: %v\n", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("⚠️  退出中...")
		cancel()
	}()
	go func() {
		time.AfterFunc(time.Second*10, func() {
			fmt.Println("group pause all!")
			group.PauseAll()
		})
	}()

	go func() {
		time.AfterFunc(time.Second*20, func() {
			fmt.Println("group resume all!")
			group.ResumeAll()
		})
	}()
	handler := ConsumerGroupHandler{
		KafkaSafeConsumer: tracker.NewKafkaSafeConsumer(),
	}

	wg := &sync.WaitGroup{}
	consumerNum := 100
	for i := range consumerNum { // 消费者数目
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
type ConsumerGroupHandler struct {
	*tracker.KafkaSafeConsumer
}

func (h ConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	go func() {
		for {
			<-time.After(5 * time.Second)
			sess.Commit()
		}
	}()
	return nil
}
func (h ConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	//sess.Commit()
	return nil
}

type WalletResponse struct {
	Data struct {
		URL    string `json:"url"`
		Key    string `json:"key"`
		Params struct {
			OrderId string `json:"order_id"`
		} `json:"params"`
		ID string `json:"id"`
	} `json:"data"`
}

type OrderNotifyMessage struct {
	Data struct {
		//Info      map[string]any `json:"info"`
		NotifyUrl string `json:"notify_url"`
		OrderType string `json:"order_type"`
		DataId    string `json:"data_id"`
	} `json:"data"`
}

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		//msgTracker.Start(msg.Offset)
		data := &OrderNotifyMessage{}
		json.Unmarshal(msg.Value, data)
		fmt.Println("resp: ", data)
		fmt.Printf("📩 收到: id=%s key=%s\n", data.Data.DataId, data.Data.OrderType)
		/* if err := requestNotify(data); err != nil {
			fmt.Printf("📩 Request notify service failed: id=%s err: %s\n", walletResp.Data.ID, err.Error())
			continue
		} */
		fmt.Printf("📩 Notify success: id=%s\n", msg.Value)
		//msgTracker.Done(msg.Offset)
		session.MarkMessage(msg, "") // 标记已消费
	}
	return nil
}

type NotifyRequest struct {
	OrderId string `json:"order_id"`
	ID      string `json:"id"`
}

func requestNotify(walletMsg *WalletResponse) error {
	params := &NotifyRequest{
		OrderId: walletMsg.Data.Params.OrderId,
		ID:      walletMsg.Data.ID,
	}
	url := walletMsg.Data.URL
	payload, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Notify service response Body:", string(body))
	return nil
}
func notifyHandler() {

}
