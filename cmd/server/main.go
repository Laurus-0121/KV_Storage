package main

import (
	"KV_Storage"
	"KV_Storage/cmd"
	"flag"
	"fmt"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"

	"os"
	"os/signal"
	"syscall"
)

func init() {
	// print banner
	banner, _ := ioutil.ReadFile("../../resource/banner.txt")
	fmt.Println(string(banner))
}

var config = flag.String("config", "", "the config file for KV_storage")
var dirPath = flag.String("dir_path", "", "the dir path for the database")

func main() {
	flag.Parse() // 解析配置

	//set the config
	var cfg KV_Storage.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = KV_Storage.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err : %+v\n", err)
			return
		}
		cfg = *c
	}

	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}

	// 监听中断事件
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := cmd.NewServer(cfg) // 新建一个server
	if err != nil {
		log.Printf("create KV_storage server err: %+v\n", err)
		return
	}
	go server.Listen(cfg.Addr) // 启动一个goroutine处理server

	<-sig
	server.Stop()
	log.Println("KV_storage is ready to exit, bye...")
}

func newConfigFromFile(config string) (*KV_Storage.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = new(KV_Storage.Config)
	err = toml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
