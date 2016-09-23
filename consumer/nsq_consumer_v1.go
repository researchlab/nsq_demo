package consumer

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
)

type msgHandler struct {
	msgChan chan *nsq.Message
	stop    bool
}

func (m *msgHandler) HandleMessage(msg *nsq.Message) error {
	if !m.stop {
		m.msgChan <- msg
	}
	return nil
}

func (m *msgHandler) Process() {
	m.stop = false
	for {
		select {
		case msg := <-m.msgChan:
			if err := doMsgWork(msg); err != nil {
				msg.Requeue(-1)
			} else {
				msg.Finish()
			}
		case <-time.After(time.Second):
			if m.stop {
				close(m.msgChan)
				return
			}
		}
	}
}

func doMsgWork(msg *nsq.Message) error {
	fmt.Println(string(msg.Body))
	return nil
}

//设定消费者超时机制
func ConsumerT(topicName, channelName, lookupAddr string) {
	consumer, err := nsq.NewConsumer(topicName, channelName, nsq.NewConfig())
	if err != nil {
		fmt.Println(err.Error())
	}
	handler := msgHandler{
		msgChan: make(chan *nsq.Message, 1024),
		stop:    false,
	}
	consumer.AddHandler(nsq.HandlerFunc(handler.HandleMessage))
	if err := consumer.ConnectToNSQLookupd(lookupAddr); err != nil {
		fmt.Println(err.Error())
	}
	handler.Process()
}
