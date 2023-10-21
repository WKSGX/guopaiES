package main

import (
    "fmt"
    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"
    "github.com/google/gopacket/pcap"
    "log"
    "os"
    "sort"
    "sync"
    "encoding/csv"
)

var (
    pcapFile   = "H:\\go_project\\001.pcap" // 指定 PCAP 文件路径
    batchSize  = 10000
    topN       = 10
    outputFile = "H:\\go_project\\top_tcp_flows.csv"
)

type TCPTuple struct {
    SrcIP   string
    DstIP   string
    SrcPort uint16
    DstPort uint16
}

type TCPFlow struct {
    Tuple TCPTuple
    Bytes int
}

func main() {
    pcapHandle, err := pcap.OpenOffline(pcapFile)
    if err != nil {
        log.Fatal(err)
    }
    defer pcapHandle.Close()

    var mu sync.Mutex
    tcpFlows := make(map[TCPTuple]int)
    packetCount := 0

    for {
        packetData, _, err := pcapHandle.ReadPacketData()
        if err != nil {
            log.Fatal(err)
        }

        packetCount++

        packet := gopacket.NewPacket(packetData, pcapHandle.LinkType(), gopacket.Default)

        if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
            tcp := tcpLayer.(*layers.TCP)
            tuple := TCPTuple{
                SrcIP:   packet.NetworkLayer().NetworkFlow().Src().String(),
                DstIP:   packet.NetworkLayer().NetworkFlow().Dst().String(),
                SrcPort: uint16(tcp.SrcPort),
                DstPort: uint16(tcp.DstPort),
            }

            mu.Lock()
            tcpFlows[tuple] += len(tcp.Payload)
            mu.Unlock()

            if packetCount%batchSize == 0 {
                // 每收到 batchSize 个包，进行一次排序并输出前 N 个 TCP 流
                topFlows := getTopTCPFlows(tcpFlows, topN)
                go writeCSV(topFlows)
            }
        }
    }
}

func getTopTCPFlows(tcpFlows map[TCPTuple]int, n int) []TCPFlow {
    topFlows := make([]TCPFlow, 0, n)
    for tuple, bytes := range tcpFlows {
        topFlows = append(topFlows, TCPFlow{Tuple: tuple, Bytes: bytes})
    }

    // 按字节数降序排序
    sort.Slice(topFlows, func(i, j int) bool {
        return topFlows[i].Bytes > topFlows[j].Bytes
    })

    // 截取前N个流
    if len(topFlows) > n {
        topFlows = topFlows[:n]
    }

    return topFlows
}

func writeCSV(topFlows []TCPFlow) {
    file, err := os.Create(outputFile)
    if err != nil {
        log.Println(err)
        return
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    // 写入 CSV 文件头
    header := []string{"Source IP", "Destination IP", "Source Port", "Destination Port", "Total Bytes"}
    if err := writer.Write(header); err != nil {
        log.Println(err)
        return
    }

    // 写入排名前 N 的 TCP 流数据
    for _, flow := range topFlows {
        record := []string{
            flow.Tuple.SrcIP,
            flow.Tuple.DstIP,
            fmt.Sprintf("%d", flow.Tuple.SrcPort),
            fmt.Sprintf("%d", flow.Tuple.DstPort),
            fmt.Sprintf("%d", flow.Bytes),
        }
        if err := writer.Write(record); err != nil {
            log.Println(err)
            return
        }
    }
}
