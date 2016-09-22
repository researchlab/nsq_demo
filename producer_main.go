package main

import (
	"fmt"
	"github.com/researchlab/nsq_demo/libs"
	"github.com/researchlab/nsq_demo/producer"
	"strconv"
	"sync"
	"time"
)

var (
	NSQDTCPADDR    = "127.0.0.1:4150" // nsqd tcp address
	TOPICNAME_LIST = []string{"p_events", "s_events", "weixin_events"}
)

func main() {

	np := &producer.NProducer{
		Nsqd_Tcp_Addr: NSQDTCPADDR,
	}
	np.ProducerCon()
	SProducerPub(np)
}

//串行生成指定数目的消息
func SProducerPub(np *producer.NProducer) {
	msg_Num := 1000000
	var wg sync.WaitGroup
	wg_len := len(TOPICNAME_LIST)
	wg.Add(wg_len)
	for _, topicName := range TOPICNAME_LIST {
		go func(np *producer.NProducer, topicName string, msg_Num int, wg *sync.WaitGroup) {
			start := time.Now()
			for i := 1; i <= msg_Num; i++ {
				np.ProducerPub(topicName, "serial_producer: "+strconv.Itoa(i))
			}
			exec_time := time.Since(start).Seconds()
			fmt.Println(msg_Num, "serial_producer Total_exec_time:", libs.ToFixed(exec_time, 9), "s;")
			wg.Done()
		}(np, topicName, msg_Num, &wg)
	}
	wg.Wait()
}

func PProducerPub(np *producer.NProducer) {
	msg_Num := 1000000
	msg_cur_Num := 10000
	var g_wg sync.WaitGroup
	g_wg_len := len(TOPICNAME_LIST)
	g_wg.Add(g_wg_len)
	for _, topicName := range TOPICNAME_LIST {
		go func(np *producer.NProducer, topicName string, msg_Num, msg_cur_Num int, g_wg *sync.WaitGroup) {
			var wg sync.WaitGroup
			wg.Add(msg_Num)
			p_chan := make(chan int, msg_cur_Num)
			start := time.Now()
			for i := 1; i <= msg_Num; i++ {
				p_chan <- 1
				go np.ProducerPubChan(topicName, "p_producer:"+strconv.Itoa(i), &wg, p_chan)
			}
			wg.Wait()
			exec_time := time.Since(start).Seconds()
			fmt.Println(msg_Num, "p_producer Total_exec_time:", libs.ToFixed(exec_time, 9), "s;")
			g_wg.Done()
		}(np, topicName, msg_Num, msg_cur_Num, &g_wg)
	}
	g_wg.Wait()
}
