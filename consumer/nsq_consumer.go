package consumer

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/researchlab/nsq_demo/libs"
	"os"
	"sync"
	"time"
)

func dealMsg(inChan chan *nsq.Message) {
	for msg := range inChan {
		err := doWork(msg)
		if err != nil {
			msg.Requeue(-1)
			continue
		}
		msg.Finish()
	}
}

func doWork(msg *nsq.Message) error {
	fmt.Println(string(msg.Body))
	return nil
}

//desc: universal consumer msg function
func ConsumerMsg(topicName, channelName, lookupAddr string, wg *sync.WaitGroup) {
	defer wg.Done()
	if k, ok := libs.CheckStrEmpty(topicName, channelName, lookupAddr); !ok {
		fmt.Printf("args[%d] is null", k)
		os.Exit(1)
	}
	inChan := make(chan *nsq.Message)
	conf := nsq.NewConfig()
	conf.Set("max-in-flight", 1000)
	consumer, err := nsq.NewConsumer(topicName, channelName, conf)
	if err != nil {
		fmt.Println("[Error NewConsumer]", err)
		panic(err)
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		inChan <- m
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd(lookupAddr); err != nil {
		fmt.Println("[Error ConnectToNSQLookupd]", err)
		panic(err)
	}

	go dealMsg(inChan)
	// read from this channel to block until consumer is cleanly stopped
	<-consumer.StopChan
}

//desc: consumer the given number msg
//@return  the total exec_time of consumer all msg.
func ConsumerGMsg(topicName, channelName, lookupAddr string, msgNum int) (exec_time float64) {
	if k, ok := libs.CheckStrEmpty(topicName, channelName, lookupAddr); !ok {
		fmt.Printf("args[%d] is null", k)
		os.Exit(1)
	}
	if k, ok := libs.CheckIntZero(msgNum); !ok {
		fmt.Printf("args[%d] is null", k)
		os.Exit(1)

	}
	wg := &sync.WaitGroup{}
	wg.Add(msgNum)

	start := time.Now()
	conf := nsq.NewConfig()
	consumer, _ := nsq.NewConsumer(topicName, channelName, conf)
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		if err := doWork(msg); err != nil {
			//			wg.Done()
			return err
		}
		wg.Done()
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd(lookupAddr); err != nil {
		fmt.Println("[Error ConnectToNSQLookupd]", err)
		panic(err)
	}
	wg.Wait()
	exec_time = time.Since(start).Seconds()
	return exec_time

}
