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

	"github.com/IBM/sarama"
)

const (
	consumerGroup = "wallet.group.test3.order.created.notify"
	topic         = "wallet.topic.test3.order.created"
	brokers       = "182.16.4.66:9092"
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
	consumerNum := 100
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

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		/* walletResp := &WalletResponse{}
		json.Unmarshal(msg.Value, walletResp)
		fmt.Println("resp: ", walletResp)
		fmt.Printf("📩 收到: id=%s key=%s\n", walletResp.Data.ID, walletResp.Data.Key)
		if err := requestNotify(walletResp); err != nil {
			fmt.Printf("📩 Request notify service failed: id=%s err: %s\n", walletResp.Data.ID, err.Error())
			continue
		} */
		fmt.Printf("📩 Notify success: id=%s\n", msg.Value)

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
