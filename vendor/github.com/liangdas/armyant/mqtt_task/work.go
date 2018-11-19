// Copyright 2014 hey Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mqtt_task

import (
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/liangdas/armyant/task"
	"github.com/liangdas/armyant/work"
	"time"
)

func NewWork(manager *Manager) *Work {
	this := new(Work)
	this.manager = manager
	//opts:=this.GetDefaultOptions("tls://127.0.0.1:3563")
	//opts := this.GetDefaultOptions("tcp://127.0.0.1:3563")
	opts := this.GetDefaultOptions("ws://127.0.0.1:3653")
	opts.SetConnectionLostHandler(func(client MQTT.Client, err error) {
		fmt.Println("ConnectionLost", err.Error())
	})
	opts.SetOnConnectHandler(func(client MQTT.Client) {
		fmt.Println("OnConnectHandler")
	})
	opts.SetTLSConfig(manager.Cert())
	err := this.Connect(opts)
	if err != nil {
		fmt.Println(err.Error())
	}
	this.On("UserCenter/CoinCount",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/CoinCount: ",string(msg.Payload()))
	})
	this.On("UserCenter/ExpCount",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/ExpCount: ",string(msg.Payload()))
	})
	this.On("FatigueCenter/FatiguePointUpper",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("FatigueCenter/FatiguePointUpper: ",string(msg.Payload()))
	})
	this.On("UserCenter/ShieldUpper",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/ShieldUpper: ",string(msg.Payload()))
	})
	this.On("UserCenter/CoinUpper",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/CoinUpper: ",string(msg.Payload()))
	})
	this.On("UserCenter/ExpUpper",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/ExpUpper: ",string(msg.Payload()))
	})
	this.On("UserCenter/ShieldCount",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("UserCenter/ShieldCount",string(msg.Payload()))
	})
	this.On("ItemCenter/ItemList",func(client MQTT.Client, msg MQTT.Message){
		fmt.Println("ItemCenter/ItemList",string(msg.Payload()))
	})

	return this
}

/**
Work 代表一个协程内具体执行任务工作者
*/
type Work struct {
	work.MqttWork
	manager  *Manager
	QPS      int
	closeSig bool
}

func (this *Work) Init(t task.Task) {
	this.QPS = 1 //每一个并发平均每秒请求次数(限流)
	this.closeSig = false

	//登陆
	msg,err := this.Request("Login@Login001/HD_Login",[]byte(`{"code":"061ZZQ2G0jNF3k2bDH1G0zAd3G0ZZQ2Y"}`))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("login: ",string(msg.Payload()))
}

/**
每一次请求都会调用该函数,在该函数内实现具体请求操作

task:=task.LoopTask{
		C:100,		//并发数
}
*/
func (this *Work) RunWorker(t task.Task) {
	for !this.closeSig {
		var throttle <-chan time.Time
		if this.QPS > 0 {
			throttle = time.Tick(time.Duration(1e6/(this.QPS)) * time.Microsecond)
		}

		if this.QPS > 0 {
			<-throttle
		}
		this.worker(t)
	}
}
func (this *Work) worker(t task.Task) {
	msg, err := this.Request("GameCenter@GameCenter001/HD_OpenBox", []byte(`{"index":1}`))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(string(msg.Payload()))
}
func (this *Work) Close(t task.Task) {
	this.closeSig = true
}
