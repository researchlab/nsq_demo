package producer

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/researchlab/nsq_demo/libs"
	//	"sync"
	"time"
)

type NProducer_VI struct {
	//nsqd server tcp address
	Nsqd_Tcp_Addr string
}

//Publish 同步发送一条消息给指定的topic, 发送失败则返回错误，否则返回nil
func (np *NProducer_VI) ProducerPub_VI(topicName, msgStr string, msg_Num int) {
	defer func() {
		if err := recover(); nil != err {
			fmt.Println("[Runtime error]", err)
			return
		}
	}()
	p_conn, err := nsq.NewProducer(np.Nsqd_Tcp_Addr, nsq.NewConfig())
	if err != nil {
		p_conn, err = nsq.NewProducer(np.Nsqd_Tcp_Addr, nsq.NewConfig())
		if err != nil {
			panic(err)
		}

	}

	// Stop initiates a graceful stop of the Producer (permanent)
	defer p_conn.Stop()
	start := time.Now()
	for i := 1; i <= msg_Num; i++ {
		if err := p_conn.Publish(topicName, []byte(msgStr)); err != nil {
			fmt.Println(err.Error())
		}
	}
	exec_time := time.Since(start).Seconds()
	fmt.Println(msg_Num, "serial_producer Total_exec_time:", libs.ToFixed(exec_time, 9), "s;")
}
