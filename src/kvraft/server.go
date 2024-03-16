package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Option   string
	Seq      int64
	ClientID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataMap              map[string]string         //存储数据
	dispatcher           map[int]chan Notification //在Raft层Apply后通知RPC
	lastAppliedRequestId map[int64]int64           //记录每个Clerk已处理的最大的SequenceId

}

type Notification struct {
	ClientID int64
	Seq      int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:      args.Key,
		Option:   "Get",
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	reply.WrongLeader = kv.WaitApplying(op, 500*time.Millisecond)

	if reply.WrongLeader == false {
		kv.mu.Lock()
		value, b := kv.DataMap[args.Key]
		kv.mu.Unlock()

		if b {
			reply.Value = value
			return
		}

		//找不到
		reply.Err = ErrNoKey
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Option:   args.Op,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	reply.WrongLeader = kv.WaitApplying(op, 500*time.Millisecond)

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.DataMap = make(map[string]string)
	kv.dispatcher = make(map[int]chan Notification)
	kv.lastAppliedRequestId = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for msg := range kv.applyCh {
			if msg.CommandValid == false {
				continue
			}

			op := msg.Command.(Op)
			DPrintf("kvserver %d start applying command %s at index %d, request id %d, client id %d",
				kv.me, op.Option, msg.CommandIndex, op.Seq, op.ClientID)

			kv.mu.Lock()
			if kv.isDuplicateRequest(op.ClientID, op.Seq) {
				kv.mu.Unlock()
				continue
			}
			switch op.Option {
			case "Put":
				kv.DataMap[op.Key] = op.Value
			case "Append":
				kv.DataMap[op.Key] += op.Value
			}

			kv.lastAppliedRequestId[op.ClientID] = op.Seq

			if ch, ok := kv.dispatcher[msg.CommandIndex]; ok {
				notify := Notification{
					ClientID: op.ClientID,
					Seq:      op.Seq,
				}
				ch <- notify
			}

			kv.mu.Unlock()
			DPrintf("kvserver %d applied command %s at index %d, request id %d, client id %d",
				kv.me, op.Option, msg.CommandIndex, op.Seq, op.ClientID)
		}
	}()

	return kv
}

func (kv *KVServer) isDuplicateRequest(ClientID int64, Seq int64) bool {
	appliedRequestId, b := kv.lastAppliedRequestId[ClientID]
	if b == false || Seq > appliedRequestId {
		return false
	}
	return true
}

func (kv *KVServer) WaitApplying(op Op, timeout time.Duration) bool {
	// return common part of GetReply and PutAppendReply
	// i.e., WrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		return true
	}

	var wrongLeader bool

	kv.mu.Lock()
	if _, ok := kv.dispatcher[index]; !ok {
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	ch := kv.dispatcher[index]
	kv.mu.Unlock()
	select {
	case notify := <-ch:
		if notify.ClientID != op.ClientID || notify.Seq != op.Seq {
			// leader has changed
			wrongLeader = true
		} else {
			wrongLeader = false
		}

	case <-time.After(timeout):
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientID, op.Seq) {
			wrongLeader = false
		} else {
			wrongLeader = true
		}
		kv.mu.Unlock()
	}
	DPrintf("kvserver %d got %s() RPC, insert op %+v at %d, reply WrongLeader = %v",
		kv.me, op.Option, op, index, wrongLeader)

	kv.mu.Lock()
	delete(kv.dispatcher, index)
	kv.mu.Unlock()
	return wrongLeader
}
