package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// 定义一个结构体来存储每一行数据和检查结果
type RowData struct {
	row    []string
	isGood bool
	index  int
}

func main() {
	startTime := time.Now()

	fileData, err := readFile()
	if err != nil {
		fmt.Printf("读取文件失败: %v\n", err)
		return
	}

	totalRows := len(fileData.Rows)
	fmt.Printf("总共需要检查 %d 条链接\n", totalRows)

	// 创建结果通道
	resultsChan := make(chan RowData, totalRows)

	// 创建等待组
	var wg sync.WaitGroup

	// 创建工作池
	workerCount := 50
	rowChan := make(chan RowData, totalRows)

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(rowChan, resultsChan, &wg)
	}

	// 发送任务
	go sendTask(fileData, rowChan)

	// 创建进度显示协程
	processed := 0
	go showProgress(&processed, totalRows)

	// 等待所有工作完成
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 收集结果
	goodRows := [][]string{fileData.Header}
	badRows := [][]string{fileData.Header}
	results := make([]RowData, totalRows)

	// 收集结果并更新进度
	for result := range resultsChan {
		results[result.index] = result
		processed++
	}

	// 按原始顺序整理结果
	for _, result := range results {
		if result.isGood {
			goodRows = append(goodRows, result.row)
		} else {
			badRows = append(badRows, result.row)
		}
	}

	// 写入文件
	if err := writeToCSV("good.csv", goodRows); err != nil {
		fmt.Printf("写入good.csv失败: %v\n", err)
	}
	if err := writeToCSV("bad.csv", badRows); err != nil {
		fmt.Printf("写入bad.csv失败: %v\n", err)
	}

	fmt.Printf("\n处理完成!\n")
	fmt.Printf("正常链接: %d 个\n", len(goodRows)-1)
	fmt.Printf("异常链接: %d 个\n", len(badRows)-1)
	fmt.Printf("总耗时: %v\n", time.Since(startTime))
}

func showProgress(processed *int, totalRows int) {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			progress := float64(*processed) / float64(totalRows) * 100
			fmt.Printf("\r当前进度: %.2f%% (%d/%d)", progress, *processed, totalRows)
		}
	}
}

func sendTask(fileData *FileData, rowChan chan<- RowData) {
	for i, row := range fileData.Rows {
		rowChan <- RowData{row: row, index: i}
	}
	close(rowChan)
}	

func worker(rowChan <-chan RowData, resultsChan chan<- RowData, wg *sync.WaitGroup) {
	defer wg.Done()
	for rowData := range rowChan {
		url := rowData.row[4]
		isGood := checkURL(url)
		resultsChan <- RowData{
			row:    rowData.row,
			isGood: isGood,
			index:  rowData.index,
		}
	}
}

func checkURL(url string) bool {
	if url == "" {
		return false
	}

	// 清理 URL
	url = strings.TrimSpace(url)

	// 如果是文件路径，标记为异常
	if strings.HasPrefix(url, "file:///") {
		return false
	}

	// 检查URL格式
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return false
	}

	// 创建自定义的 HTTP 客户端
	client := &http.Client{
		Timeout: 10 * time.Second,
		// CheckRedirect 用于处理HTTP重定向
		// req 是即将发送的请求
		// via 包含已经发送的请求,最新的请求在最后
		// 如果重定向次数超过10次,返回最后一个响应以防止无限重定向
		// 否则返回nil允许继续重定向
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	// 创建GET请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	// 添加请求头
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Connection", "close")

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// 检查状态码
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		return true
	}

	// 特殊处理某些返回403的情况
	if resp.StatusCode == 403 {
		// 可能是防爬虫，等待一秒后重试
		time.Sleep(time.Second)
		resp2, err := client.Do(req)
		if err != nil {
			return false
		}
		defer resp2.Body.Close()
		return resp2.StatusCode >= 200 && resp2.StatusCode < 400
	}

	return false
}

func writeToCSV(filename string, rows [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("写入行失败: %v", err)
		}
	}

	return nil
}

type FileData struct {
	Header []string
	Rows   [][]string
}

func readFile() (*FileData, error) {
	file, err := os.Open("data.csv")
	if err != nil {
		fmt.Println("打开文件失败", err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	header, err := reader.Read()
	if err != nil {
		fmt.Printf("读取表头失败: %v\n", err)
		return nil, err
	}

	// 读取所有数据
	var rows [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}

	return &FileData{
		Header: header,
		Rows:   rows,
	}, nil
}