/**
 * Copyright (c) 2014-2015, GoBelieve
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import "net"
import "sync"
import "time"
import log "github.com/golang/glog"

// 每次同步打包消息大小
const SYNC_BATCH_SIZE = 1000

// 同步客户端,分配给从库(从库作为主库的客户端)
type SyncClient struct {
	conn *net.TCPConn
	ewt  chan *Message // 主库下发的消息
}

func NewSyncClient(conn *net.TCPConn) *SyncClient {
	c := new(SyncClient)
	c.conn = conn
	c.ewt = make(chan *Message, 10)
	return c
}

// 从库客户端的运行处理
func (client *SyncClient) RunLoop() {
	seq := 0
	// 在规定的期限内,第一次必须收到从服务器发来`MSG_STORAGE_SYNC_BEGIN的`消息
	msg := ReceiveMessage(client.conn)
	if msg == nil {
		return
	}
	if msg.cmd != MSG_STORAGE_SYNC_BEGIN {
		return
	}

	// 第一条消息的body是个cursor,记录了从服务器的需要同步的消息id,详细键Slaver.RunOnce
	cursor := msg.body.(*SyncCursor)
	log.Info("cursor msgid:", cursor.msgid)
	c := storage.LoadSyncMessagesInBackground(cursor.msgid)
	// 将自cursor之后的消息发送同步给从库
	// 第一次是一条一条发过去,避免打包发送包太大
	for batch := range c {
		msg := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
		seq = seq + 1
		msg.seq = seq
		SendMessage(client.conn, msg)
	}
	// 添加从库记录
	master.AddClient(client)
	defer master.RemoveClient(client)

	// 循环等待处理主服务器新消息
	for {
		msg := <-client.ewt
		// 是nil是服务器主动关闭
		if msg == nil {
			log.Warning("chan closed")
			break
		}
		// 否则就是需要同步给客户端的消息
		seq = seq + 1
		msg.seq = seq
		err := SendMessage(client.conn, msg)
		if err != nil {
			break
		}
	}
}

func (client *SyncClient) Run() {
	go client.RunLoop()
}

type Master struct {
	ewt chan *EMessage

	mutex   sync.Mutex
	clients map[*SyncClient]struct{}
}

func NewMaster() *Master {
	master := new(Master)
	master.clients = make(map[*SyncClient]struct{})
	master.ewt = make(chan *EMessage, 10)
	return master
}

func (master *Master) AddClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.clients[client] = struct{}{}
}

func (master *Master) RemoveClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	delete(master.clients, client)
}

func (master *Master) CloneClientSet() map[*SyncClient]struct{} {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	clone := make(map[*SyncClient]struct{})
	for k, v := range master.clients {
		clone[k] = v
	}
	return clone
}

// 消息打包下发
func (master *Master) SendBatch(cache []*EMessage) {
	if len(cache) == 0 {
		return
	}

	// 因为主库cache是1000,所以初始化容量1000
	batch := &MessageBatch{msgs: make([]*Message, 0, SYNC_BATCH_SIZE)}
	batch.first_id = cache[0].msgid
	for _, em := range cache {
		batch.last_id = em.msgid
		batch.msgs = append(batch.msgs, em.msg)
	}
	m := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
	clients := master.CloneClientSet()
	for c := range clients {
		c.ewt <- m
	}
}

// 主库运行
func (master *Master) Run() {
	cache := make([]*EMessage, 0, SYNC_BATCH_SIZE)
	var first_ts time.Time
	for {
		// 消息至多每秒同步一次
		// 然后每次阻塞不超过60秒(这似乎没什么意义)
		t := 60 * time.Second
		if len(cache) > 0 {
			ts := first_ts.Add(time.Second * 1)
			now := time.Now()

			if ts.After(now) {
				t = ts.Sub(now)
			} else {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
		select {
		case emsg := <-master.ewt:
			cache = append(cache, emsg)
			if len(cache) == 1 {
				first_ts = time.Now()
			}
			if len(cache) >= SYNC_BATCH_SIZE {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		case <-time.After(t):
			if len(cache) > 0 {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
	}
}

func (master *Master) Start() {
	go master.Run()
}

type Slaver struct {
	addr string // 这是对应主库的地址
}

func NewSlaver(addr string) *Slaver {
	s := new(Slaver)
	s.addr = addr
	return s
}

// 从库运行
func (slaver *Slaver) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	// 这个seq目前没什么用途,单纯的记录
	seq := 0

	msgid := storage.NextMessageID()
	cursor := &SyncCursor{msgid}
	log.Info("cursor msgid:", msgid)
	// 首先发送开始标志给主库,随后主库会将同步msgid至当前的消息发送过来
	msg := &Message{cmd: MSG_STORAGE_SYNC_BEGIN, body: cursor}
	seq += 1
	msg.seq = seq
	SendMessage(conn, msg)

	for {
		// 收到主库的消息,消息大小限制32M,也就是最大如果1000条发来,平均每条32k
		// 出错了就会创建新连接,继续同步
		msg := ReceiveStorageSyncMessage(conn)
		if msg == nil {
			return
		}
		// 一开始同步的单条消息
		if msg.cmd == MSG_STORAGE_SYNC_MESSAGE {
			emsg := msg.body.(*EMessage)
			storage.SaveSyncMessage(emsg)
		} else if msg.cmd == MSG_STORAGE_SYNC_MESSAGE_BATCH {
			// 打包消息
			mb := msg.body.(*MessageBatch)
			storage.SaveSyncMessageBatch(mb)
		} else {
			log.Error("unknown message cmd:", Command(msg.cmd))
		}
	}
}

func (slaver *Slaver) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", slaver.addr)
		// 如果连接错误,一直重连知道连上为止
		if err != nil {
			log.Info("connect master server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("slaver sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("slaver connected with master")
		nsleep = 100
		slaver.RunOnce(tconn)
	}
}

func (slaver *Slaver) Start() {
	go slaver.Run()
}
