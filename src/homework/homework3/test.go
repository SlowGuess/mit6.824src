package main

import "time"

type Peer struct {
	id int
	currentTerm int
	state int //0-follower,1-candidate,3-leader

	votedFor int
	vote_granted bool


	Election_Timeout_Timer time.Ticker
	Heartbeat_Timer time.Ticker
	Election_Timeout_Timer_Flag int
	Heartbeat_Timer_Flag int
}

func toFollower(peer Peer)Peer{//转变为Follower
	peer.state = 1
	peer.Election_Timeout_Timer_Flag =1
	peer.Election_Timeout_Timer_Flag = 1

	peer.Election_Timeout_Timer.Restart

	return peer
}

func toCandidate(peer Peer)Peer{

}

func toLeader(peer Peer)Peer{

}

func initialpeer(NodeNumber int){//初始化i个节点

}

func follower_handler(follower peer){
	接收消息
	if 消息from leader,即心跳信息{
		if 消息任期>=follower.currentTerm{
		follower.term = 消息任期
		follower.Election_Timeout_Timer.Restart
		follower.votedFor = 消息.id
		follower.vote_granted = true
		}else{
			follower.votedFor = 消息.id
			follower.vote_granted = false
		}
	}
	if 消息from candidate,即心跳信息{
		//处理逻辑
	}


	}

	if 超时定时器timeout {//这里应该写成后台检查
		toCandidate(follower)
		follower.currentTerm++
		go func candidate_handler()
	}
}


