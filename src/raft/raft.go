package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	rand2 "math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// AppendEntriesRequest 只有leader才能发送的心跳包
type AppendEntriesRequest struct {
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 我是哪台机器, 编号 [0,n)   实际上就是leader的编号

	// 2B、2C
	PrevLogIndex      int        // Leader节点认为该Follower节点已有的上一条日志条目的索引,从0开始
	PrevLogTerm       int64      // PrevLogIndex 的任期
	LeaderCommitIndex int        // 自己的 commitIndex 值
	Entries           []LogEntry // 要复制过去的日志
}

type LogEntry struct {
	Term    int64       // 任期
	Index   int         // 在日志list中的索引号，从1开始
	Command interface{} // 具体的数据，在这里面就是命令
	ID      int64       // 全局唯一id
}

// AppendEntriesReply 收到心跳后的回复结构体
type AppendEntriesReply struct {
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 回复一下自己是哪台机器, 编号 [0,n)

	// 2B、2C
	Success    bool // 如果跟随者包含与prevLogIndex和prevLogTerm匹配的条目，则为true。
	MatchIndex int  // 告诉leader最后一次复制完的日志index
	HasReplica bool // follower是否复制了日志, 仅用于debug打印
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Term             int64  // 维护的任期，初始化的时候是1
	Role             int32  // 角色  1-follower  2-candidate  3-leader
	VotedFor         int32  // 投票给了谁 编号 [0,n)， 初始化时为-1
	PeersVoteGranted []bool // 被投票的结果,leader才有

	RequestVoteTimeTicker          *time.Ticker // 投票循环计时器，follower和candidate才有
	RequestVoteDuration            time.Duration
	RequestAppendEntriesTimeTicker *time.Ticker // rpc循环计时器，leader才有
	RequestAppendEntriesDuration   time.Duration

	//2B
	Log         []LogEntry // 日志数组   在语义上第一个下标为1而不是0
	CommitIndex int        // 已经提交的最大的日志index， 初始化为0

	ApplyCh     chan ApplyMsg // 检测程序所用，提交之后的日志发送到这里
	LastApplied int           // 最后一个被应用到[完成提交]状态机的日志下标, 实验2B用不到

	NextIndex  []int // leader才有意义。对于各个raft节点，下一个需要接收的日志条目的索引，初始化为自己最后一个log的下标+1
	MatchIndex []int // leader才有意义。对于各个raft节点，已经复制过去的最高的日志下标【正常是从1开始，所以这里初始化是0】

	// 实验2D
	LastIncludedTerm  int64 // 最后快照保存的term
	LastIncludedIndex int   // 最后快照保存的index, 初始化为0. 之后正常从1开始
}

const (
	// RoleFollower 角色  1-follower  2-candidate  3-leader
	RoleFollower  = 1
	RoleCandidate = 2
	RoleLeader    = 3
	InitVoteFor   = -1 // 初始化投票为空
	InitTerm      = 1  // 初始化任期
	//BaseRPCCyclePeriod 一轮rpc周期基线，在论文中有推荐的值，每个实例在这个基础上新增 0~ RPCRandomPeriod 毫秒的随机值
	BaseRPCCyclePeriod = 40 * time.Millisecond
	// BaseElectionCyclePeriod 一轮选举周期基线，在论文中有推荐的值，每个实例在这个基础上新增 0~ ElectionRandomPeriod 毫秒的随机值
	BaseElectionCyclePeriod = 300 * time.Millisecond

	RPCRandomPeriod      = 10
	ElectionRandomPeriod = 100
)

// GlobalID 全局自增ID，需要原子性自增，用于debug
var GlobalID = int64(100)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.Term)
	if rf.Role == RoleLeader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.Term)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.CommitIndex)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.Term)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	d.Decode(&rf.CommitIndex)

	Info("%d号机器恢复信息成功!!!", rf.me)

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).

}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // 自己这里维护的任期
	ServerNumber int32 // 我是哪台机器, 编号 [0,n)

	LastLogIndex int   // 2B 所持有的最后一条日志记录的 index, 从1开始
	LastLogTerm  int64 // 2B LastLogIndex 的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Agree        bool  // 同意还是拒绝
	Term         int64 // 回复一下自己维护的任期
	ServerNumber int32 // 回复一下自己是哪台机器, 编号 [0,n)
}

// RequestVote 处理别人的选举请求
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.ServerNumber = int32(rf.me)
	reply.Term = rf.Term

	// 是自己的话直接同意并跳过
	// todo your code
	if int(args.ServerNumber) == rf.me {
		reply.Agree = true
		rf.VotedFor = int32(rf.me)
		return
	}

	Trace("%+v号机器收到%+v号机器的投票请求, 自己的任期是%+v请求中的任期是%+v自己的VotedFor:%+v      LastLogIndex:%+v   LastLogTerm:%+v",
		rf.me, args.ServerNumber, rf.Term, args.Term, rf.VotedFor, args.LastLogIndex, args.LastLogTerm)

	// 论文原文 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	//一旦任期小都会反对
	if args.Term < rf.Term {
		reply.Agree = false
		Warning("%+v号机器反对了%+v号机器选举", rf.me, args.ServerNumber)
		return
	}

	//无论是什么身份，遇到任期比自己大的都跟随
	if args.Term > rf.Term {
		rf.convert2Follower(args.Term)
		reply.Term = rf.Term

		//跟随前要判断日志是否比自己新
		if args.LastLogIndex != 0 {
			if len(rf.Log) != 0 {
				if (rf.Log[len(rf.Log)-1].Term > args.LastLogTerm) || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && rf.Log[len(rf.Log)-1].Index > args.LastLogIndex) {
					//如果leader最后一条日志的任期比自己小，或任期相同但索引比自己小，则自己的日志新，拒绝跟随
					reply.Agree = false
					Warning("%+v号机器因【日志比leader新】反对了%+v号机器选举,自己的日志是%+v", rf.me, args.ServerNumber, rf.Log)
					return
				}
			}
		} else {
			//处理越界情况
			if len(rf.Log) != 0 {
				reply.Agree = false
				Warning("%+v号机器因【日志比leader新】反对了%+v号机器选举,自己的日志是%+v", rf.me, args.ServerNumber, rf.Log)
				return
			}
		}

		//注意这里需要重置自己的选举计时器
		rf.RequestVoteTimeTicker.Reset(BaseElectionCyclePeriod + time.Duration(rand2.Intn(ElectionRandomPeriod)*int(time.Millisecond)))

		//赞成投票
		reply.Agree = true
		rf.VotedFor = args.ServerNumber

		Success("%+v号机器赞成了%+v号机器选举", rf.me, args.ServerNumber)
		return
	}

	if args.Term == rf.Term {
		//任期相等时只有follower节点没投票时才会赞成(动画逻辑)
		//任期相等时无论follower还是candidate都可投票
		//if rf.Role == RoleFollower {
		if rf.VotedFor == InitVoteFor || rf.VotedFor == args.ServerNumber {
			rf.Role = RoleFollower

			//跟随前要判断日志是否比自己新
			if args.LastLogIndex != 0 {
				if (rf.Log[len(rf.Log)-1].Term > args.LastLogTerm) || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && rf.Log[len(rf.Log)-1].Index > args.LastLogIndex) {
					//如果leader最后一条日志的任期比自己小，或任期相同但索引比自己小，则自己的日志新，拒绝跟随
					reply.Agree = false
					Warning("%+v号机器因【日志比leader新】反对了%+v号机器选举,自己的日志是%+v", rf.me, args.ServerNumber, rf.Log)
					return
				}
			} else {
				//处理越界情况
				if len(rf.Log) != 0 {
					reply.Agree = false
					Warning("%+v号机器因【日志比leader新】反对了%+v号机器选举,自己的日志是%+v", rf.me, args.ServerNumber, rf.Log)
					return
				}
			}

			rf.RequestVoteTimeTicker.Reset(BaseElectionCyclePeriod + time.Duration(rand2.Intn(ElectionRandomPeriod)*int(time.Millisecond)))
			reply.Agree = true
			rf.VotedFor = args.ServerNumber

			Success("%+v号机器赞成了%+v号机器选举", rf.me, args.ServerNumber)
			return
		}
	}
	//}

}

// AsyncBatchSendRequestVote 非领导者并行发送投票请求，收到响应后进行处理
// candidate发起投票
func (rf *Raft) AsyncBatchSendRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Info("candidate:%+v 开始发起选举投票,当前任期为:%+v", rf.me, rf.Term)

	for index, _ := range rf.peers {
		//如果是已经表示赞成自己的节点，则不用再向其发送
		//if rf.PeersVoteGranted[index] {
		//	continue
		//}

		if rf.me == index {
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.Term,
			ServerNumber: int32(rf.me),

			LastLogIndex: len(rf.Log),
		}
		if len(rf.Log) != 0 {
			args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
		}

		reply := &RequestVoteReply{}
		Trace("%+v号机器发送选主请求, 发给%v号,自己的信息是:%v", rf.me, index, *args)
		go func(i int) {
			if flag := rf.sendRequestVote(i, args, reply); !flag {
				//  网络原因，需要重发，这里先不实现
				//util.Error(fmt.Sprint("网络原因【选举】发送不成功！！！", rf.me, "号机器发送选举请求给", i, "号机器没有成功, 任期为 ", rf.Term))
			} else {
				rf.HandleRequestVoteResp(args, reply)
			}
		}(index)
	}
}

// 请求投票的req被返回了，处理一下
func (rf *Raft) HandleRequestVoteResp(req *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.Term {
		rf.convert2Follower(reply.Term)
		//此时不需要重置选举计时器,也无需反馈信息
		Warning("%+v号机器收到%+v号机器的投票回复%+v，任期为%+v，选择跟随,自己的Role为:%+v", rf.me, reply.ServerNumber, reply.Agree, reply.Term, rf.Role)
		return
	}

	Success("%+v号机器收到%+v号机器的投票回复%+v，任期为%+v，自己的Role:%+v", rf.me, reply.ServerNumber, reply.Agree, reply.Term, rf.Role)

	rf.PeersVoteGranted[reply.ServerNumber] = reply.Agree
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.PeersVoteGranted[i] == true {
			count++
		}
	}
	if count > len(rf.peers)/2 { // 如果自己被投了超过1/2票，那么转换成 leader, 然后启动后台 backupGroundRPCCycle 心跳线程
		rf.convert2Leader()
		rf.RequestAppendEntriesTimeTicker.Reset(BaseRPCCyclePeriod + time.Duration(rand2.Intn(RPCRandomPeriod)*int(time.Millisecond)))
		go rf.backupGroundRPCCycle()
		Info("%+v号机器任期为%+v，当选为leader,自己的Role:%+v", rf.me, rf.Term, rf.Role)
	}

}

// 启动心跳后台任务。只有 ledaer 才会发心跳
func (rf *Raft) backupGroundRPCCycle() {
	// 一直循环下去，直到实例退出
	for atomic.LoadInt32(&rf.dead) != 1 && atomic.LoadInt32(&rf.Role) == RoleLeader {
		select {
		case <-rf.RequestAppendEntriesTimeTicker.C:
			// 根据角色不同，去发rpc请求
			switch atomic.LoadInt32(&rf.Role) {
			case RoleLeader:
				rf.AsyncBatchSendRequestAppendEntries()
				//发送后立即重置心跳定时器 //也可以不重置，在实验中影响不大
				rf.RequestAppendEntriesTimeTicker.Reset(BaseRPCCyclePeriod + time.Duration(rand2.Intn(RPCRandomPeriod)*int(time.Millisecond)))
			default:
				// 不是leader了，结束后台线程
				return
			}
		}
	}
}

// AsyncBatchSendRequestAppendEntries 异步并行发送多个心跳包
func (rf *Raft) AsyncBatchSendRequestAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role != RoleLeader {
		return
	}

	Info("leader：%+v 开始发送心跳，任期为：%+v", rf.me, rf.Term)
	Error("%+v号leader的日志情况为：%+v", rf.me, rf.Log)

	for index, _ := range rf.peers {
		if index == rf.me {
			continue // 跳过自己
		}

		args := &AppendEntriesRequest{
			Term:         rf.Term,
			ServerNumber: int32(rf.me),

			PrevLogIndex:      rf.NextIndex[index] - 1,
			LeaderCommitIndex: rf.CommitIndex,
		}
		if len(rf.Log) != 0 {
			//Error("index:%+v,  rf.NextIndex[index]-1:%+v,  rf.Log:%+v", index, rf.NextIndex[index]-1, rf.Log)
			args.Entries = rf.Log[rf.NextIndex[index]-1:]
		}
		if args.PrevLogIndex != 0 && args.PrevLogIndex <= len(rf.Log) {
			args.PrevLogTerm = rf.Log[args.PrevLogIndex-1].Term
		}

		reply := &AppendEntriesReply{}
		go func(i int) {
			Trace("%+v号机器向%+v号机器发送心跳，args信息：%+v", rf.me, i, *args)
			//util.Trace(fmt.Sprint(rf.me, "号机器开始发送心跳给", i, "号机器, 任期为 ", rf.Term, "  req为", fmt.Sprintf("%+v %s", *args, logPrint)))
			if flag := rf.sendRPCAppendEntriesRequest(i, args, reply); !flag {
				//  网络原因，需要重发
				//util.Error(fmt.Sprint("网络原因【心跳】发送不成功！！！", rf.me, "号机器发送心跳给", i, "号机器没有成功, 任期为 ", rf.Term))
			} else {
				rf.HandleAppendEntriesResp(args, reply)
			}
		}(index)

	}
}

// HandleAppendEntriesResp 心跳 req 被返回了，处理一下
func (rf *Raft) HandleAppendEntriesResp(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//看一下任期是否比自己大，比自己大则降级跟随
	if reply.Term > rf.Term {
		rf.convert2Follower(reply.Term)
		//此时需要重置选举计时器
		rf.RequestVoteTimeTicker.Reset(BaseElectionCyclePeriod + time.Duration(rand2.Intn(ElectionRandomPeriod)*int(time.Millisecond)))
		return
	}

	if rf.Role != RoleLeader {
		return
	}

	count := 0
	if reply.Success {
		//处理matchIndex和nextIndex
		rf.MatchIndex[reply.ServerNumber] = reply.MatchIndex
		rf.NextIndex[reply.ServerNumber] = reply.MatchIndex + 1

		for i, matchIndex := range rf.MatchIndex {
			//Error("i:%+v,matchIndex:%+v",i,matchIndex)
			if i == rf.me { //跳过统计自己
				continue
			}
			//Error("matchIndex:%+v,reply.MatchIndex:%+v",matchIndex,reply.MatchIndex)
			if matchIndex >= reply.MatchIndex {
				count++
			}
		}

	} else if rf.Term == reply.Term {
		// 如果leader发送的 AppendEntries 因为日志不一致而失败，减少 nextIndex 并重试. 根据follower的实现中，失败原因只有两种，一个是term不一致一个是日志不一致
		oldNextIndex := rf.NextIndex[reply.ServerNumber]
		//错误处理,nextIndex指针回退一格（可优化如二分查找，这里简单实现）
		rf.NextIndex[reply.ServerNumber] = max(1, reply.MatchIndex+1)
		rf.MatchIndex[reply.ServerNumber] = reply.MatchIndex
		Warning(fmt.Sprint(rf.me, "号ld减少%d号机器的nextIndex从", oldNextIndex, "到", rf.NextIndex[reply.ServerNumber], " reply.MatchIndex:", reply.MatchIndex), reply.ServerNumber)
	}

	commitFlag := false
	oldCommitIndex := rf.CommitIndex

	// 提交限制判断
	if len(rf.Log) != 0 && reply.MatchIndex != 0 && rf.Log[reply.MatchIndex-1].Term != rf.Term {
		Error("%+v号leader由于提交限制无法提交", rf.me)
		return
	}

	//如果日志被复制超过1/2个节点的话，执行提交，这里由于跳过了自己，默认count多+1
	Error("count:%+v,半数:%+v,reply.MatchIndex:%+v", count, len(rf.peers)/2, reply.MatchIndex)
	if count+1 > len(rf.peers)/2 && rf.CommitIndex < reply.MatchIndex {
		if rf.CommitIndex+1 <= len(rf.Log) {
			rf.CommitIndex = max(rf.CommitIndex+1, reply.MatchIndex)
		}
		commitFlag = true
	}

	Error("oldcommitIndex:%+v,commitIndex:%+v!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", oldCommitIndex, rf.CommitIndex)
	if commitFlag {
		for i := oldCommitIndex; i <= rf.CommitIndex-1; i++ {
			//if 需要提交的时候
			Error("%+v号leader提交日志成功,提交到index为:%+v", rf.me, rf.CommitIndex)
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: rf.Log[i].Index,
			}

		}
	}

}

// AppendEntries 收到心跳包，如何回应
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.ServerNumber = int32(rf.me)
	Trace("%+v号机器收到%+v号机器的心跳信息, 自己的任期是%+v请求中的任期是%+v自己的VotedFor%+v,日志信息为:%+v", rf.me, req.ServerNumber, rf.Term, req.Term, rf.VotedFor, *req)

	// todo 这个 else 可以放到最前面，并直接 return ， 另外可以加一条打印
	//收到任期小于自己，包反对
	if req.Term < rf.Term {
		reply.Term = rf.Term
		reply.Success = false
		reply.HasReplica = false
		Warning("%+v号机器的任期更大，拒绝了%+v号机器发出的心跳", rf.me, req.ServerNumber)
		return
	}

	//收到任期大于等于自己，则都选择跟随，这里2A实验 candidate与follower情况相同，不作分类,在之后实验可能需要修改
	if rf.Term < req.Term {
		rf.convert2Follower(req.Term)
	}
	// 之后一定是  req.Term == rf.Term
	rf.Role = RoleFollower
	rf.RequestVoteTimeTicker.Reset(rf.RequestVoteDuration)
	reply.Term = rf.Term

	//回应心跳
	reply.Success = true
	rf.VotedFor = req.ServerNumber

	//————————————————进行日志处理
	//初始没有日志的情况，prevLogIndex==0
	if req.PrevLogIndex == 0 && len(rf.Log) == 0 {
		reply.Success = true
		reply.MatchIndex = len(req.Entries)
		for _, pojo := range req.Entries {
			rf.Log = append(rf.Log, LogEntry{
				Term:    pojo.Term,
				Index:   len(rf.Log) + 1, // index语义从1开始
				Command: pojo.Command,
				ID:      pojo.ID,
			})
			reply.HasReplica = true
		}
		Success("%+v号机器回复%+v号机器发出的心跳，结果是:%+v,初始没有日志，日志复制情况为:%+v", rf.me, req.ServerNumber, reply.Success, reply.HasReplica)
		return
	}
	// todo 第二步：如果自己日志的此下标没有，或者任期和预期的不一样，返回false
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex != 0 {
		if req.PrevLogIndex > len(rf.Log) || rf.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// todo 正确赋值 reply.MatchIndex
			//reply.MatchIndex = req.PrevLogIndex - 1
			reply.MatchIndex = rf.CommitIndex
			reply.Success = false
			reply.HasReplica = false
			Warning(fmt.Sprint(rf.me, "机器收到", req.ServerNumber, "的心跳【发生日志冲突】", " CommitIndex:", rf.CommitIndex, fmt.Sprintf(" req:%+v reply:%+v Log:%+v", *req, *reply, rf.Log)))
			return
		}
	}

	// todo 第三步：如果自己的日志和req中的发生任期冲突，删除所有已有的index之后的
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	//if req.PrevLogTerm != rf.Log[req.PrevLogIndex-1].Term {
	for _, pojo := range req.Entries {
		// 删除自己本下标之后不一致的所有日志
		//Error("pojo:%+v !!!!!!", pojo)
		if pojo.Index <= len(rf.Log) {
			if rf.Log[pojo.Index-1].Term != pojo.Term {
				Warning(fmt.Sprint(rf.me, "机器丢弃日志，因为ld心跳中的日志", ",值为", rf.CommitIndex, fmt.Sprintf(" reply:%+v 丢弃的Log是%+v", *reply, rf.Log[pojo.Index-1-rf.LastIncludedIndex])))
				rf.Log = rf.Log[:pojo.Index-1]
				Warning("%+v修改后的日志信息为:%+v", rf.me, rf.Log)
			}
		}
	}

	// todo 第四步，添加日志
	// 4. Append any new entries not already in the log
	for _, pojo := range req.Entries {
		// 不要重复添加
		if pojo.Index > len(rf.Log) {
			// 不应该取 req 日志中的 index， 要重新弄成自己的index
			rf.Log = append(rf.Log, LogEntry{
				Term:    pojo.Term,
				Index:   len(rf.Log) + 1, // index语义从1开始
				Command: pojo.Command,
				ID:      pojo.ID,
			})
			reply.HasReplica = true
		}
	}

	// todo 设置一下 reply.MatchIndex
	reply.MatchIndex = req.PrevLogIndex + len(req.Entries)

	// todo 第五步 如果req中leaderCommit > 自己的commitIndex，令 commitIndex 等于 leaderCommit 和最后一个新日志记录的 index 值之间的最小值
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	commitIndexUpdate := req.LeaderCommitIndex > rf.CommitIndex && len(rf.Log) != 0
	oldCommitIndex := rf.CommitIndex
	if commitIndexUpdate {
		rf.CommitIndex = min(req.LeaderCommitIndex, rf.Log[len(rf.Log)-1].Index)
	}

	// todo 第六步，当 CommitIndex 更新时，相当于提交，需要给检测程序发送
	if commitIndexUpdate {
		for i := oldCommitIndex; i <= rf.CommitIndex-1; i++ {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: rf.Log[i].Index,
			}
			rf.LastApplied = rf.CommitIndex
		}
	}

	Success("%+v号机器回复%+v号机器发出的心跳，结果是:%+v,日志复制情况为：%+v，自己的日志信息是:%+v", rf.me, req.ServerNumber, reply.Success, reply.HasReplica, rf.Log)
	//Error("%+v号机器的日志情况为：%+v", rf.me, rf.Log)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 模仿上面，发送心跳rpc
func (rf *Raft) sendRPCAppendEntriesRequest(server int, req *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

// 转换为 follower 用的函数
func (rf *Raft) convert2Follower(term int64) {
	rf.Role = RoleFollower
	rf.Term = term
	rf.VotedFor = InitVoteFor
	rf.PeersVoteGranted = make([]bool, len(rf.peers))
}

// 转换为 leader 用的函数
func (rf *Raft) convert2Leader() {
	rf.Role = RoleLeader
	rf.VotedFor = int32(rf.me)
	rf.PeersVoteGranted = make([]bool, len(rf.peers))

	//初始化leader的nextIndex 和 matchIndex数组
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = rf.CommitIndex + 1
		rf.MatchIndex[i] = 0
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, false
	}
	//是leader
	rf.Log = append(rf.Log, LogEntry{
		Term:    int64(term),
		Index:   len(rf.Log) + 1,
		Command: command,
		ID:      atomic.AddInt64(&GlobalID, 1),
	})
	index = len(rf.Log)
	Warning("往%+v号机器的目录中添加了一条日志，该日志在目录中的Index为：%+v", rf.me, index)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 后台线程，选举超时计时器逻辑
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		// 先不考虑 rpc 的超时控制，因为 sendRequestVote 说自己考虑过了。
		case <-rf.RequestVoteTimeTicker.C:
			// 时间到了
			rf.mu.Lock()
			if rf.dead == 1 {
				return
			}
			switch rf.Role {
			case RoleFollower, RoleCandidate:
				// 转换成 candidate 并且 term+1
				rf.Role = RoleCandidate
				rf.Term++
				// 重置一下投票的结果
				rf.PeersVoteGranted = make([]bool, len(rf.peers))
				rf.PeersVoteGranted[rf.me] = true
				go rf.AsyncBatchSendRequestVote()

			case RoleLeader:
				// 不用做
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Term = InitTerm
	rf.Role = RoleFollower
	rf.VotedFor = InitVoteFor
	rf.ApplyCh = applyCh
	rf.PeersVoteGranted = make([]bool, len(rf.peers))
	rf.PeersVoteGranted[rf.me] = true

	rf.RequestVoteDuration = BaseElectionCyclePeriod + time.Duration(rand2.Intn(ElectionRandomPeriod))*time.Millisecond
	rf.RequestAppendEntriesDuration = BaseRPCCyclePeriod + time.Duration(rand2.Intn(RPCRandomPeriod))*time.Millisecond

	rf.RequestVoteTimeTicker = time.NewTicker(rf.RequestVoteDuration)
	rf.RequestAppendEntriesTimeTicker = time.NewTicker(rf.RequestAppendEntriesDuration)

	rf.Log = []LogEntry{}
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = rf.CommitIndex + 1
		rf.MatchIndex[i] = 0
	}

	fmt.Println(rf.me, "号机器的选举循环周期是", rf.RequestVoteDuration.Milliseconds(),
		"毫秒", "  rpc周期是", rf.RequestAppendEntriesDuration.Milliseconds(), "毫秒")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ————————————————————————————————    用于颜色打印      ———————————————————————————————————————————————

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
	color_magenta //洋红
	info          = "[INFO]"
	trac          = "[TRAC]"
	erro          = "[ERRO]"
	warn          = "[WARN]"
	succ          = "[SUCC]"
)

// see complete color rules in document in https://en.wikipedia.org/wiki/ANSI_escape_code#cite_note-ecma48-13
func Trace(format string, a ...interface{}) {
	prefix := yellow(trac)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Info(format string, a ...interface{}) {
	prefix := blue(info)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Success(format string, a ...interface{}) {
	prefix := green(succ)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Warning(format string, a ...interface{}) {
	prefix := magenta(warn)
	fmt.Println(formatLog(prefix), fmt.Sprintf(format, a...))
}
func Error(format string, a ...interface{}) {
	prefix := red(erro)
	fmt.Println(formatLog(prefix), red(fmt.Sprintf(format, a...)))
}
func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}
func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}
func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}
func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}
func magenta(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_magenta, s)
}
func formatLog(prefix string) string {
	return time.Now().Format("2006/01/02 15:04:05.000") + " " + prefix + " "
}
