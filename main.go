package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	//_ "github.com/cdle/sillyGirl/develop/boltdb"

	"github.com/DeanThompson/ginpprof"
	"github.com/beego/beego/v2/core/logs"
	"github.com/cdle/sillyGirl/core"
	"github.com/cdle/sillyGirl/utils"
)

func main() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	time.Local = loc
	core.Init()
	ginpprof.Wrapper(core.Server)
	sillyGirl := core.MakeBucket("sillyGirl")
	if sillyGirl.GetBool("anti_kasi") {
		go utils.MonitorGoroutine()
	}
	port := sillyGirl.GetString("port", "8080")
	logs.Info("Http服务已运行(%s)。", sillyGirl.GetString("port", "8080"))
	go core.Server.Run("0.0.0.0:" + port)
	logs.Info("关注频道 https://t.me/kczz2021 获取最新消息。")
	// logs.Info("机器码：%s", core.GetMachineID())
	d := false
	for _, arg := range os.Args {
		if arg == "-d" {
			d = true
		}
	}
	if !d {
		t := false
		for _, arg := range os.Args {
			if arg == "-t" {
				t = true
			}
		}
		if t {
			logs.Info("终端交互已启用。")
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				data := scanner.Text()
				f := &core.Faker{
					Type:    "terminal",
					Message: string(data),
					Carry:   make(chan string),
					Admin:   true,
				}
				core.Senders <- f
				go func() {
					for v := range f.Listen() {
						fmt.Printf("\x1b[%dm%s \x1b[0m\n", 31, v)
					}
				}()
			}
			logs.Info("终端交互异常,请检查运行环境设置,如果是docker环境,请附加-it参数")
		} else {
			logs.Info("终端交互不可用，运行带-t参数即可启用。")
		}
	}

	select {}
}
