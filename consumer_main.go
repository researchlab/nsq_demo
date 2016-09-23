package main

import (
	"fmt"
	"github.com/researchlab/nsq_demo/consumer"
	"sync"
)

var (
	TOPICNAME_LIST   = []string{"p_events", "s_events", "weixin_events"}
	CHANNELNAME_LIST = []string{"encode", "decode", "dev1.0-info"}
)

var (
	TOPICNAME   = "s_events"
	CHANNELNAME = "getInfo"
	LOOKUPADDR  = "127.0.0.1:4161" //连接nsqlookupd 服务发现暴露的http ip:port
)

func main() {
	//consumerGMsg()
	//consumerMsg()
	consumerTimeOut()
}

func consumerTimeOut() {
	consumer.ConsumerT(TOPICNAME, CHANNELNAME, LOOKUPADDR)
}

//desc 发起 TOPICNAME_LIST * CHANNELNAME_LIST 个消费者,
//只有主动中止或异常中止，否在这些消费者一直消费消息
//直到消费完所有消息，并等着消费即将进入的消息
func consumerMsg() {
	var wg sync.WaitGroup
	wg_len := len(TOPICNAME_LIST) * len(CHANNELNAME_LIST)
	wg.Add(wg_len)
	for _, topicName := range TOPICNAME_LIST {
		for _, channelName := range CHANNELNAME_LIST {
			go consumer.ConsumerMsg(topicName, channelName, LOOKUPADDR, &wg)
		}
	}
	wg.Wait()
}

//@return 返回消费给定数目消息的执行时间
func consumerGMsg() {
	msg_Num := 10000

	exec_time := consumer.ConsumerGMsg(TOPICNAME, CHANNELNAME, LOOKUPADDR, msg_Num)

	fmt.Printf("%d consumer exec_time: %f s", msg_Num, exec_time)
}
