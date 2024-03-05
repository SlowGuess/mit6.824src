package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 计算服务
type MathService struct {
	candy []string
	pear  []string
	mu    sync.Mutex
}

// 计算服务提供的方法
func (a *MathService) Dispatcher(req *CandyPearRequest, resp *CandyPearResponse) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.candy) == 0 {
		log.Println("糖果已分完")

		// 等待3秒钟
		time.Sleep(3 * time.Second)
		if len(a.pear) == 0 {
			log.Println("梨子也已分完")
			return nil
		} else {
			amount2 := req.Y
			if amount2 > len(a.pear) {
				amount2 = len(a.pear)
			}
			resp.Result = a.pear[:amount2]
			a.pear = a.pear[amount2:]
		}
	} else {
		amount1 := req.X

		if amount1 >= len(a.candy) {
			amount1 = len(a.candy)

			resp.Result = a.candy[:amount1]
			a.candy = a.candy[amount1:]

			if len(a.pear) == 0 {
				log.Println("梨子已分完")
				return nil
			} else {
				amount2 := req.Y
				if amount2 > len(a.pear) {
					amount2 = len(a.pear)
				}
				resp.Result = append(resp.Result, a.pear[:amount2]...)
				a.pear = a.pear[amount2:]

			}
		} else {
			resp.Result = a.candy[:amount1]
			a.candy = a.candy[amount1:]
		}
	}

	return nil
}

type CandyPearRequest struct {
	X, Y int
}

type CandyPearResponse struct {
	Result []string
}

func main() {
	math := &MathService{
		candy: []string{"Candy1", "Candy2", "Candy3", "Candy4", "Candy5"},
		pear:  []string{"Pear1", "Pear2", "Pear3", "Pear4", "Pear5"},
	}

	//math := new(MathService)
	rpc.Register(math)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		//协议填错或端口已被绑定
		log.Fatal("listen error:", err)
	}

	ticker := time.NewTicker(time.Second * 1) //创建一个定时器，用于10s内无用户索取则关闭

	for i := 1; i <= 10; i++ {
		select {
		case <-ticker.C:
			// 10s 内没有客户端来索要，结束服务端进程
			fmt.Printf("当前秒数:%d\n", i)

		default:

			conn, err := listener.Accept()
			if err != nil {
				// 一般是网络断开或连接超时
				log.Fatal("accept error:", err)
			}
			if conn != nil { // 有客户连接
				i = 1 // 重置计时器，因为有新的客户端连接
				fmt.Println("收到新连接,重新计时")
				go func() {
					defer conn.Close()
					rpc.ServeConn(conn)
				}()
			}
		}
	}
	fmt.Println("10秒内无客户端连接，服务端进程结束")
	os.Exit(0)
}
