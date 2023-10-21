package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/olivere/elastic/v7"
)

type TransactionLog struct {
	Line      int       `json:"line"`
	Channel   int       `json:"channel"`
	Timestamp time.Time `json:"timestamp"`
	Length    int       `json:"length"`
	SrcIP     string    `json:"srcIp"`
	DstIP     string    `json:"dstIp"`
	SrcPort   int       `json:"srcPort"`
	DstPort   int       `json:"dstPort"`
	Type      int       `json:"type"`
	MainType  string    `json:"mainType"`
	BidNumber string    `json:"bidNumber"`
	BidTime   time.Time `json:"bidTime"`
	HTTP      struct{}  `json:"http"`
	Stomp     struct {
		Command string `json:"command"`
	} `json:"stomp"`
}

var (
	esClient *elastic.Client
	err      error
	esHost   = "https://10.176.22.16:9200"
	username = "elastic" // 修改为你的 Elasticsearch 用户名
	password = "guopaiES123"
	index    = "transactionlog-new-2023-07-06" // 修改为你的索引名称
)

func main() {
	// 创建 Elasticsearch 客户端
	esClient, err = elastic.NewClient(
		elastic.SetURL(esHost),
		elastic.SetBasicAuth(username, password),
		elastic.SetHealthcheck(true),
		elastic.SetSniff(false),
		// 设置 Elasticsearch 客户端忽略证书验证
		elastic.SetHttpClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}),
	)
	if err != nil {
		log.Fatalf("无法连接到 Elasticsearch: %s\n", err)
		return
	}

	// 延迟关闭 Elasticsearch 客户端
	defer esClient.Stop()

	// 等待一段时间以确保连接成功
	// 可根据实际情况调整等待时间
	// 在连接较慢的环境中可能需要更长的等待时间
	// 也可以使用循环来等待 Elasticsearch 可用
	// 这里仅作示例
	waitTime := 1 * time.Second
	time.Sleep(waitTime)

	// 创建查询以获取索引的所有文档
	query := elastic.NewMatchAllQuery()

	// 执行查询
	ctx := context.Background()
	searchResult, err := esClient.Search().
		Index(index).
		Query(query).
		Do(ctx)
	if err != nil {
		log.Fatalf("查询出错: %s\n", err)
		return
	}

	for _, hit := range searchResult.Hits.Hits {
		// 解析 JSON 数据为 TransactionLog 结构体
		var transaction TransactionLog
		err := json.Unmarshal(hit.Source, &transaction)
		if err != nil {
			log.Fatalf("JSON 解码错误: %s\n", err)
			continue
		}

		// 输出 TransactionLog 结构体中的字段
		log.Printf("Line: %d\n", transaction.Line)
		log.Printf("Channel: %d\n", transaction.Channel)
		log.Printf("Timestamp: %s\n", transaction.Timestamp)
		log.Printf("Length: %d\n", transaction.Length)
		log.Printf("SrcIP: %s\n", transaction.SrcIP)
		log.Printf("DstIP: %s\n", transaction.DstIP)
		log.Printf("SrcPort: %d\n", transaction.SrcPort)
		log.Printf("DstPort: %d\n", transaction.DstPort)
		log.Printf("Type: %d\n", transaction.Type)
		log.Printf("MainType: %s\n", transaction.MainType)
		log.Printf("BidNumber: %s\n", transaction.BidNumber)
		log.Printf("BidTime: %s\n", transaction.BidTime)
		log.Printf("HTTP: %+v\n", transaction.HTTP)
		log.Printf("Stomp Command: %s\n", transaction.Stomp.Command)
	}

	// 处理查询结果，输出时间戳
	// for _, hit := range searchResult.Hits.Hits {
	// 	// 直接输出原始 JSON 数据
	// 	jsonData, err := json.Marshal(hit.Source)
	// 	if err != nil {
	// 		log.Fatalf("JSON 编码错误: %s\n", err)
	// 		continue
	// 	}
	// 	if err = hit.Source.UnmarshalJSON(jsonData); err != nil {
	// 		log.Fatalf("JSON 解码错误: %s\n", err)
	// 		continue
	// 	}
	// 	// 打印 JSON 数据
	// 	log.Println(string(jsonData))
	// }

}
