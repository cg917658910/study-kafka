package main

import (
	"context"
	"fmt"
	"time"
)

type consumerHandler interface {
	ConsumeMessage(ch <-chan *Message) error
}

type MyHandler struct{}

func (h *MyHandler) ConsumeMessage(ch <-chan *Message) error {
	for msg := range ch {
		// 处理消息
		fmt.Println("Received message:", msg.Topic)
	}
	return nil
}

type Message struct {
	Topic string
}

type nofitySvc struct {
	hdCh chan *Message
}

func newNotifySvc() *nofitySvc {
	return &nofitySvc{
		hdCh: make(chan *Message, 100),
	}
}

func (n *nofitySvc) send(msg *Message) {
	n.hdCh <- msg
}
func (n *nofitySvc) stop() {
	close(n.hdCh)
}
func (n *nofitySvc) consume(handler consumerHandler) error {
	go handler.ConsumeMessage(n.hdCh)
	return nil
}

func main() {

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	timer := time.AfterFunc(time.Second*5, func() {
		fmt.Println("time.After func do")
	})
	fmt.Println("time.after next")

	select {
	case <-ctx.Done():
		timer.Stop()
	case <-time.After(time.Second * 6):
		timer.Stop()
	}
	return
	notifySvc := newNotifySvc()
	handler := &MyHandler{}
	if err := notifySvc.consume(handler); err != nil {
		fmt.Println("Error consuming messages:", err)
		return
	}
	// 模拟发送消息
	for i := range 10 {
		msg := &Message{Topic: fmt.Sprintf("topic-%d", i)}
		notifySvc.send(msg)
	}
	defer notifySvc.stop()      // 关闭消息通道
	time.Sleep(2 * time.Second) // 等待消息处理完成
	//select {}
}
