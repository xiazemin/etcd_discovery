package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

//创建租约注册服务
type ServiceRegister struct {
	etcdClient *clientv3.Client             //etcd client
	lease      clientv3.Lease               //租约
	leaseResp  *clientv3.LeaseGrantResponse //设置租约时间返回
	canclefunc func()                       //租约撤销
	//租约keepalieve相应chan
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string //注册的key
}

func NewServiceRegister(addr []string, timeNum int64) (*ServiceRegister, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}

	var (
		client *clientv3.Client
	)

	//连接etcd
	if clientTem, err := clientv3.New(conf); err == nil {
		client = clientTem
	} else {
		return nil, err
	}

	ser := &ServiceRegister{
		etcdClient: client,
	}

	//申请租约设置时间keepalive
	if err := ser.setLease(timeNum); err != nil {
		return nil, err
	}

	//监听续租相应chan
	go ser.ListenLeaseRespChan()
	return ser, nil
}

//设置租约
func (this *ServiceRegister) setLease(timeNum int64) error {
	//申请租约
	lease := clientv3.NewLease(this.etcdClient)

	//设置租约时间
	leaseResp, err := lease.Grant(context.TODO(), timeNum)
	if err != nil {
		return err
	}

	//设置续租 定期发送需求请求
	ctx, cancelFunc := context.WithCancel(context.TODO())
	leaseRespChan, err := lease.KeepAlive(ctx, leaseResp.ID)

	if err != nil {
		return err
	}

	this.lease = lease
	this.leaseResp = leaseResp
	this.canclefunc = cancelFunc
	this.keepAliveChan = leaseRespChan
	return nil
}

//监听 续租情况
func (this *ServiceRegister) ListenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-this.keepAliveChan:
			if leaseKeepResp == nil {
				fmt.Printf("已经关闭续租功能\n")
				return
			} else {
				fmt.Printf("续租成功\n")
			}
		}
	}
}

//通过租约 注册服务
func (this *ServiceRegister) PutService(key, val string) error {
	//带租约的模式写入数据即注册服务
	kv := clientv3.NewKV(this.etcdClient)
	_, err := kv.Put(context.TODO(), key, val, clientv3.WithLease(this.leaseResp.ID))
	return err
}

//撤销租约
func (this *ServiceRegister) RevokeLease() error {
	this.canclefunc()
	time.Sleep(2 * time.Second)
	_, err := this.lease.Revoke(context.TODO(), this.leaseResp.ID)
	return err
}

func main() {
	ser, _ := NewServiceRegister([]string{"127.0.0.1:2379"}, 5)
	ser.PutService("/server/node1", "node1")
	select {}
}
