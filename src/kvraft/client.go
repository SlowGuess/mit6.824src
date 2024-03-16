package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
}

type Req struct {
	Key   string
	Value string
	Op    string

	ClientID int64
	Seq      int64
}

type Resp struct {
	Value string
	Error Err
}

var GlobalID = int64(100)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	req := &Req{
		Key:      key,
		Seq:      atomic.AddInt64(&GlobalID, 1),
		ClientID: ck.ClientID,
	}

	i := 0

	for {
		resp := &Resp{}
		b := ck.servers[i].Call("KVServer.Get", req, resp)

		//如果key值不存在,则跳出循环，返回空值
		if resp.Error == ErrNoKey {
			return ""
		}

		if !b || resp.Error != "" {
			//出现错误，需要重试
			i = (i + 1) % len(ck.servers)
			continue
		}

		return resp.Value
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := &Req{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      atomic.AddInt64(&GlobalID, 1),
		ClientID: ck.ClientID,
	}

	i := 0

	for {
		resp := &Resp{}
		b := ck.servers[i].Call("KVServer.PutAppend", req, resp)
		if !b || resp.Error != "" {
			//出现错误，需要重试
			i = (i + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
