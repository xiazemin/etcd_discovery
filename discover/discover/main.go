package main

import (
	"context"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	//"go.etcd.io/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type ServiceDiscovery struct {
	client     *clientv3.Client
	serverList map[string]string
	lock       sync.Mutex
}

func NewServiceDiscovery(addr []string) (*ServiceDiscovery, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	if client, err := clientv3.New(conf); err == nil {
		return &ServiceDiscovery{
			client:     client,
			serverList: make(map[string]string),
		}, nil
	} else {
		return nil, err
	}
}

func (this *ServiceDiscovery) GetService(prefix string) ([]string, error) {
	//使用key前桌获取所有的etcd上所有的server
	resp, err := this.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	//解析出所有的server放入本地
	addrs := this.extractAddrs(resp)

	//warch server前缀 将变更写入本地
	go this.watcher(prefix)
	return addrs, nil
}

// 监听key前缀
func (this *ServiceDiscovery) watcher(prefix string) {
	//监听 返回监听事件chan
	rch := this.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				this.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				this.DelServiceList(string(ev.Kv.Key))
			}
		}
	}
}

func (this *ServiceDiscovery) extractAddrs(resp *clientv3.GetResponse) []string {
	addrs := make([]string, 0)
	if resp == nil || resp.Kvs == nil {
		return addrs
	}
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			this.SetServiceList(string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			addrs = append(addrs, string(v))
		}
	}
	return addrs
}

func (this *ServiceDiscovery) SetServiceList(key, val string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.serverList[key] = string(val)
	log.Println("set data key :", key, "val:", val)
}

func (this *ServiceDiscovery) DelServiceList(key string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.serverList, key)
	log.Println("del data key:", key)
}

func (this *ServiceDiscovery) SerList2Array() []string {
	this.lock.Lock()
	defer this.lock.Unlock()
	addrs := make([]string, 0)

	for _, v := range this.serverList {
		addrs = append(addrs, v)
	}
	return addrs
}

func main() {
	cli, _ := NewServiceDiscovery([]string{"127.0.0.1:2379"})
	cli.GetService("/server")
	select {}
}
