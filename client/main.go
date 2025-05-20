package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const ()

var _client *http.Client

func init() {
	tr := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	_client = &http.Client{Transport: tr}

}
func main() {

	wg := &sync.WaitGroup{}
	// 并发生产  个线程，每个线程不断发送消息
	num := 100
	for i := 1; i <= num; i++ {
		wg.Add(1)
		go produceOrder(i, wg)
	}
	wg.Wait()
	//select {} // 阻塞主线程
}

func produceOrder(groupId int, wg *sync.WaitGroup) {
	defer wg.Done()
	maxNum := 1000
	current := 0
	for {
		if current == maxNum {
			break
		}
		current++
		key := fmt.Sprintf("%d-%d", groupId, current)
		//创建订单
		fmt.Printf("Create Order: key=%s\n", key)
		if err := requestApiGatewayTest(key); err != nil {
			fmt.Printf("Request Create Order failed: %s\n", err.Error())
			continue
		}
		fmt.Printf("Create Order success: key=%s\n", key)
	}
}

func requestApiGatewayTest(key string) error {
	targetUrl := "http://localhost:8080/test"

	u, err := url.ParseRequestURI(targetUrl)
	if err != nil {
		return err
	}
	// URL param
	data := url.Values{}
	data.Set("key", key)

	u.RawQuery = data.Encode() // URL encode

	resp, err := _client.Get(u.String())
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}
func requestCreateOrder(key string) error {
	targetUrl := "http://localhost:8061/kafka/create"

	u, err := url.ParseRequestURI(targetUrl)
	if err != nil {
		return err
	}
	// URL param
	data := url.Values{}
	data.Set("key", key)

	u.RawQuery = data.Encode() // URL encode

	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}
