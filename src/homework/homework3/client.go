package main

import (
	"log"
	"net/rpc"
	"time"
)

type CandyPearRequest struct {
	X, Y int
}

type CandyPearResponse struct {
	Result []string
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing error:", err)
	}
	args := CandyPearRequest{X: 2, Y: 3}
	reply := CandyPearResponse{}

	// 服务名.方法名。请求体可以不是指针，返回体一定要传入指针
	err = client.Call("MathService.Dispatcher", args, &reply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}
	log.Println("Result:", reply)

	if len(reply.Result) == 0 {
		log.Println("等待3秒后请求梨子...")
		time.Sleep(3 * time.Second)

		args = CandyPearRequest{X: 0, Y: 3}
		err = client.Call("MathService.Dispatcher", args, &reply)
		if err != nil {
			log.Fatal("RPC error:", err)
		}

		log.Println("Result:", reply.Result)
	}

}
