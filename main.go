package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"os/exec"
	_ "path/filepath"
	"sync"
)

type StreamConfig struct {
	URL       string `json:"url"`
	OutputDir string `json:"outputDir"`
}

type Config struct {
	Streams []StreamConfig `json:"streams"`
}

var ctx = context.Background()

func streamFramesToRedis(rtspURL string, channelName string, wg *sync.WaitGroup, rdb *redis.Client) {
	defer wg.Done()

	cmd := exec.Command("ffmpeg", "-i", rtspURL, "-vf", "fps=10", "-f", "image2pipe", "-vcodec", "mjpeg", "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Ошибка stdout ffmpeg (%s): %v\n", channelName, err)
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Printf("Ошибка запуска ffmpeg (%s): %v\n", channelName, err)
		return
	}

	reader := bufio.NewReader(stdout)
	var buffer bytes.Buffer
	inFrame := false

	for {
		b, err := reader.ReadByte()
		if err != nil {
			fmt.Printf("Ошибка чтения из stdout (%s): %v\n", channelName, err)
			break
		}

		if !inFrame {
			if b == 0xFF {
				next, err := reader.Peek(1)
				if err == nil && next[0] == 0xD8 {
					buffer.Reset()
					buffer.WriteByte(b)
					b2, _ := reader.ReadByte()
					buffer.WriteByte(b2)
					inFrame = true
				}
			}
		} else {
			buffer.WriteByte(b)

			if b == 0xFF {
				next, err := reader.Peek(1)
				if err == nil && next[0] == 0xD9 {
					b2, _ := reader.ReadByte()
					buffer.WriteByte(b2)

					err := rdb.RPush(ctx, channelName, buffer.Bytes()).Err()
					if err != nil {
						fmt.Printf("Ошибка отправки в Redis (%s): %v\n", channelName, err)
					}

					inFrame = false
				}
			}
		}
	}
}

func main() {
	configFile := os.Getenv("CONFIG_FILE")
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Printf("Ошибка открытия конфигурационного файла: %v\n", err)
		return
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		fmt.Printf("Ошибка разбора JSON: %v\n", err)
		return
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	var wg sync.WaitGroup
	for i, stream := range config.Streams {
		channel := fmt.Sprintf("stream%d", i+1)
		wg.Add(1)
		go streamFramesToRedis(stream.URL, channel, &wg, rdb)
	}

	wg.Wait()
}
