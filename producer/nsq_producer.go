package producer

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync"
)

type NProducer struct {
	//nsqd server  tcp address
	Nsqd_Tcp_Addr string
}

var p_global *nsq.Producer

func (np *NProducer) ProducerCon() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("[Runtime error]", err)
			return
		}
	}()
	var err error
	if nil == p_global {
		p_global, err = nsq.NewProducer(np.Nsqd_Tcp_Addr, nsq.NewConfig())
		if err != nil {
			p_global, err = nsq.NewProducer(np.Nsqd_Tcp_Addr, nsq.NewConfig())
			if err != nil {
				panic(err)
			}
		}
	}
}

//Publish 同步发送一条消息给指定的topic, 发送失败则返回错误，否在返回nil
func (np *NProducer) ProducerPub(topicName, msgStr string) {
	defer func() {
		if err := recover(); nil != err {
			fmt.Println("[Runtime error]", err)
			return
		}
	}()
	if nil == p_global {
		np.ProducerCon()
	}
	if err := p_global.Publish(topicName, []byte(msgStr)); err != nil {
		fmt.Println("[Error] nsq.Publish msgStr=", msgStr)
		panic(err)
	}
}

//desc 可以在指定并发频率len(p_chan)下，向指定的topic并发发送消息
func (np *NProducer) ProducerPubChan(topicName, msgStr string, wg *sync.WaitGroup, p_chan chan int) {
	if err := p_global.Publish(topicName, []byte(msgStr)); err != nil {
		fmt.Println("[Error] nsq.Publish msgStr=", msgStr)
		panic(err)
	}
	<-p_chan
	wg.Done()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
//func (np *NProducer) ProducerAsyncPub(topicName, msgStr string) {
//	if nil == p_global {
//		np.ProducerCon()
//	}
//	if err := p_global.PublishAsync(topicName, []byte(msgStr)); err != nil {
//		fmt.Println("[Error] nsq.PublishAsync msgStr=", msgStr)
//		panic(err)
//
//	}
//}
