package main

import (
	"context"
	"fmt"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/zyjblockchain/sandy_log/log"
	"os"
	"os/signal"
	"syscall"
	"tezos_index/puller"
)

func main() {
	// 0. 初始化日志级别、格式、是否保存到文件
	log.Setup(log.LevelDebug, false, true)

	ctx := context.Background()
	// 1. env
	env := puller.NewEnvironment()

	// migration database
	env.UpgradeSchema()

	crawler := env.NewPuller()
	// init
	if err := crawler.Init(ctx, puller.MODE_SYNC); err != nil {
		panic(fmt.Errorf("init crawler error : %v", err))
	}
	// puller
	crawler.Start()
	defer func() {
		// close indexer
		_ = crawler.GetIndexer().Close()
		crawler.Stop(ctx)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	<-c
}
