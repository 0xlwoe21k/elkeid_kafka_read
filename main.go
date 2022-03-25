package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
	"kfk/mq"
	"os"
	"os/signal"
	"time"
)

// kafka consumer

var (
	addr  = ""
	topic = ""
	sasl  = "false"
	user  = ""
	pass  = ""
)

func init() {
	flag.StringVar(&addr, "h", "10.43.48.22:9092", "kafka address.")
	flag.StringVar(&topic, "t", "hids_svr", "kafka topic.")
	flag.StringVar(&sasl, "s", "true", "use sasl.")
	flag.StringVar(&user, "u", "admin", "sasl_username.")
	flag.StringVar(&pass, "p", "elkeid", "sasl_password.")

}

func main() {
	flag.Parse()

	cfg := sarama.NewConfig()
	if sasl == "true" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = user
		cfg.Net.SASL.Password = pass
		cfg.Net.DialTimeout = 5 * time.Second
	}

	fmt.Println(addr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumer, err := sarama.NewConsumer([]string{addr}, cfg)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			md := &mq.MQData{}
			if err = proto.Unmarshal(msg.Value, md); err != nil {
				panic(err)
			}
			fmt.Println("KEY: %s VALUE: %s", msg.Key, md)

		case <-signals:
			os.Exit(0)
		}
	}

	//for partition := range partitionList { // 遍历所有的分区
	//	// 针对每个分区创建一个对应的分区消费者
	//	pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
	//	if err != nil {
	//		fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
	//		return
	//	}
	//	defer pc.AsyncClose()
	//	// 异步从每个分区消费信息
	//	md:=&mq.MQData{}
	//	go func(sarama.PartitionConsumer) {
	//		for msg := range pc.Messages() {
	//			if err = proto.Unmarshal(msg.Value,md); err != nil {
	//				panic(err)
	//			}
	//			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, msg.Key, md)
	//		}
	//	}(pc)
	//}
}
