package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
)

type TransactionLog struct {
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
	lastMinuteTimestampStr  = "21:38"
	firstMinuteTimestampStr = "21:35"
	esClient                *elastic.Client
	err                     error
	esHost                  = "https://10.176.22.16:9200"
	username                = "elastic" // 修改为您的 Elasticsearch 用户名
	password                = "guopaiES123"
	index                   = "transactionlog-new-2023-07-06" // 修改为您的索引名称
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

	parts := strings.Split(index, "-")
	firstTime := fmt.Sprintf("%s-%s-%sT%s:%02d",
		parts[len(parts)-3],
		parts[len(parts)-2],
		parts[len(parts)-1],
		firstMinuteTimestampStr,
		0,
	)
	secondTime := fmt.Sprintf("%s-%s-%sT%s:%02d",
		parts[len(parts)-3],
		parts[len(parts)-2],
		parts[len(parts)-1],
		lastMinuteTimestampStr,
		0,
	)
	millionSecondCounter := 0
	startTime0 := fmt.Sprintf("%s.%03d", firstTime, millionSecondCounter)
	startTime1 := fmt.Sprintf("%s.%03d", secondTime, millionSecondCounter)
	fmt.Println(startTime0)
	fmt.Println(startTime1)

	// startTime := fmt.Sprintf("%s.%03d", secondTime, millionSecondCounter)
	startTime := "2023-07-06T13:00:00.000+08:00"
	nextRangeStartTime := "2023-07-06T14:00:00.000+08:00"

	// 获取 Jul 6, 2023 @ 13:37:48.200 的前100秒时间范围

	// 获取最后10秒接收到的文档
	documents, err := searchDocuments(startTime, nextRangeStartTime)
	if err != nil {
		log.Fatalf("查询出错: %s\n", err)
		return
	}

	// 统计每秒不同线路的出价包数量
	bidCounts := make(map[string]int)

	for _, doc := range documents {
		key := fmt.Sprintf("%s-%d", doc.Timestamp.String(), doc.Channel)
		bidCounts[key] += 1
	}

	// 输出结果
	for key, count := range bidCounts {
		fmt.Printf(" Line Key: %v, Count: %v\n", key, count)
	}
	fmt.Println("输出完毕")
}

func parseUnixTimestamp(key string) int64 {
	timestampStr := key[0:100]
	timestamp, err := time.Parse("2006-01-02T15:04:05Z", timestampStr)
	if err != nil {
		log.Fatalf("Failed to parse unix timestamp from key: %v", key)
	}
	return timestamp.Unix()
}

func searchDocuments(startTime string, endTime string) ([]TransactionLog, error) {
	query := elastic.NewBoolQuery().
		Must(elastic.NewRangeQuery("timestamp").Gte(startTime).Lte(endTime))
	// query := elastic.NewMatchAllQuery()
	ctx := context.Background()
	searchResult, err := esClient.Search().
		Index(index).
		Query(query).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	var documents []TransactionLog

	for _, hit := range searchResult.Hits.Hits {
		var transaction TransactionLog
		err := json.Unmarshal(hit.Source, &transaction)
		if err != nil {
			log.Fatalf("JSON 解码错误: %s\n", err)
			continue
		}
		documents = append(documents, transaction)
	}

	return documents, nil
}
