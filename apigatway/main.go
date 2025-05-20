package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

type NotifyParams struct {
	OrderId string `json:"order_id"`
	ID      string `json:"id"`
}
type NotifyResp struct {
	NewOrderId string `json:"new_order_id"`
	ID         string `json:"id"`
}

func notifyHandler(c echo.Context) error {

	params := &NotifyParams{}
	if err := c.Bind(params); err != nil {
		return fmt.Errorf("Invalid query params: %s", err.Error())
	}
	// 调用 update
	newOrderId := strings.ToUpper(params.OrderId)
	data := &NotifyResp{
		ID:         params.ID,
		NewOrderId: newOrderId,
	}
	// 请求通知回调
	if err := notifyResult(data); err != nil {
		return fmt.Errorf("Notify Result callback error: %s", err.Error())
	}
	return c.JSON(http.StatusOK, echo.Map{
		"code": 0,
		"data": data,
	})
}

func notifyResult(params *NotifyResp) error {
	var notifyResultURL = "http://localhost:8061/kafka/update"
	payload, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", notifyResultURL, bytes.NewBuffer(payload))
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
	//_, _ := io.ReadAll(resp.Body)
	return nil
}
func testHandler(c echo.Context) error {

	return c.JSON(http.StatusOK, echo.Map{
		"code": 0,
		"msg":  "ok",
	})
}
func main() {
	//创建Echo服务的客户端
	e := echo.New()
	e.POST("/notify", notifyHandler)
	e.GET("/test", testHandler)
	fmt.Println("API Gateway 运行在端口 8080")
	e.Start(":8080")

}
