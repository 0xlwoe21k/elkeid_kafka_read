package main

import (
	"encoding/json"
	"flag"
	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
	"kfk/mq"
	"log"
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

	golog := log.New(os.Stdout, "[Elkeid]:", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

	cfg := sarama.NewConfig()
	if sasl == "true" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = user
		cfg.Net.SASL.Password = pass
		cfg.Net.DialTimeout = 5 * time.Second
	}

	//fmt.Println(addr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumer, err := sarama.NewConsumer([]string{addr}, cfg)
	if err != nil {
		golog.Printf("fail to start consumer, err:%v\n", err)
		return
	}


	// get partitionId list
	partitions, err := consumer.Partitions("my_topic")
	if err != nil {
		panic(err)
	}
	for _, partitionId := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, partitionId, sarama.OffsetOldest) // 根据topic取到所有的分区
		if err != nil {
			golog.Printf("fail to get list of partition:err%v\n", err)
			return
		}

		go func(pc *sarama.PartitionConsumer) {
			var adata = make(map[string]interface{})
			for msg := range (*pc).Messages() {
				md := &mq.MQData{}
				if err = proto.Unmarshal(msg.Value, md); err != nil {
					golog.Printf(err.Error())
				}
				adata["data_type"] = md.DataType
				adata["timestamp"] = md.Timestamp
				adata["agent_id"] = md.AgentId
				adata["in_ipv4_list"] = md.InIpv4List
				adata["ex_ipv4_list"] = md.ExIpv4List
				adata["in_ipv6_list"] = md.InIpv6List
				adata["ex_ipv6_list"] = md.ExIpv6List
				adata["hostname"] = md.Hostname
				adata["version"] = md.Version
				adata["product"] = md.Product
				adata["time_pkg"] = md.TimePkg
				adata["psm_name"] = md.PsmName
				adata["psm_path"] = md.PsmPath
				adata["tags"] = md.Tags

				if md.Body == nil{
					os.Exit(0)
				}
				for k,v:=range md.Body.Fields{
					adata[k] = v
				}
				buf ,err:= json.Marshal(adata)
				if err != nil{
					golog.Fatalln(err)
				}
				golog.Println(string(buf))
			}
		}(&partitionConsumer)

	}
	select {
	case <-signals:
		os.Exit(0)
	}

}
