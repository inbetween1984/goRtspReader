package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type StreamConfig struct {
	URL       string `json:"url"`
	OutputDir string `json:"outputDir"`
}

type Config struct {
	Streams []StreamConfig `json:"streams"`
}

func saveFrames(rtspURL, outputDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Ошибка создания директории %s: %v\n", outputDir, err)
		return
	}

	outputPattern := filepath.Join(outputDir, "frame_%04d.jpg")
	cmd := exec.Command("ffmpeg", "-i", rtspURL, "-vf", "fps=10", outputPattern)

	if err := cmd.Run(); err != nil {
		fmt.Printf("Ошибка запуска FFmpeg для %s: %v\n", rtspURL, err)
		return
	}

	fmt.Printf("Кадры успешно сохранены для %s\n", rtspURL)
}

func main() {
	configFile := "config.json"
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

	var wg sync.WaitGroup
	for _, stream := range config.Streams {
		wg.Add(1)
		go saveFrames(stream.URL, stream.OutputDir, &wg)
	}

	wg.Wait()
	fmt.Println("Все потоки обработаны")
}
