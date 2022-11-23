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
	CommandType string
	Key         string
	Value       string
	ClientId    int64
	CommandId   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	clientReply map[int64]int               //key是client的id，value是命令的id以及回复，用于检测command的重复
	replyChMap  map[int]chan ApplyNotifyMsg //key是命令在raft层的index，value是命令的回复，用于接收raft的回复
	db          db
}

//包含所有的信息返回参数，记录返回信息的模板
type ApplyNotifyMsg struct {
	Err   Err
	Value string //只有get用到
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				//kv.ApplySnapshot(applyMsg)
			}
		}
	}
}

func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex

	msg := ApplyNotifyMsg{}
	if op.CommandType == "Get" && kv.clientReply[op.ClientId] < op.CommandId {
		value, err := kv.db.get(op.Key)
		//kv.clientReply[op.ClientId] = op.CommandId
		msg.Value = value
		msg.Err = err
	} else if op.CommandType == "Put" && kv.clientReply[op.ClientId] < op.CommandId {
		kv.db.put(op.Key, op.Value)
		kv.clientReply[op.ClientId] = op.CommandId
		msg.Err = OK
	} else if op.CommandType == "Append" && kv.clientReply[op.ClientId] < op.CommandId {
		kv.db.append(op.Key, op.Value)
		kv.clientReply[op.ClientId] = op.CommandId
		msg.Err = OK
	}

	if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
		return
	}

	if kv.replyChMap[applyMsg.CommandIndex] == nil {
		//fmt.Println(kv.me, "!!!!!!!!!!!!!!!!!!!!!!!!!!")
		return
	}

	kv.replyChMap[index] <- msg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		CommandType: "Get",
		Key:         args.Key,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}

	index, _, isLeader := kv.rf.Start(op)

	if isLeader != true {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()

	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
		reply.Value = replyMsg.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	close(replyCh)
	delete(kv.replyChMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	if kv.clientReply[args.ClientId] >= args.CommandId {
		reply.Err = ErrSameCommand
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		CommandType: args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println(kv.me, "before raft!")
	kv.mu.Lock()
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()

	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
	case <-time.After(500 * time.Millisecond):
		//fmt.Println(kv.me, "time out!")
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	close(replyCh)
	delete(kv.replyChMap, index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.clientReply = make(map[int64]int)
	kv.db.m = make(map[string]string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.ReceiveApplyMsg()
	return kv
}
