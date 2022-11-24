package raft

import (
	"6.824/labgob"
	"bytes"
)

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTime = rf.setElectionTime()
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex <= rf.log[len(rf.log)-1].Index {
		//fmt.Println(lastIncludedIndex, rf.logBaseIndex)
		logs := append([]Entry(nil), rf.log[lastIncludedIndex-rf.logBaseIndex+1:]...)
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{-1, 0, 0})
		rf.log = append(rf.log, logs...)
	} else {
		rf.log = append([]Entry(nil), Entry{-1, 0, 0})
	}

	rf.logBaseTerm = lastIncludedTerm
	rf.logBaseIndex = lastIncludedIndex
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.saveStateAndSnapshot(snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logBaseIndex || index > rf.commitIndex {
		return
	}
	//fmt.Println("start snapshot", rf.commitIndex)
	rf.logBaseTerm = rf.log[len(rf.log)-1].Term
	logs := rf.log[index-rf.logBaseIndex+1:]
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{-1, 0, 0})
	rf.log = append(rf.log, logs...)
	rf.logBaseIndex = index
	rf.lastApplied = max(rf.lastApplied, rf.logBaseIndex)
	rf.snapshot = snapshot

	rf.saveStateAndSnapshot(snapshot)
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.term)
	e1.Encode(rf.log)
	e1.Encode(rf.voteFor)
	e1.Encode(rf.logBaseIndex)
	e1.Encode(rf.logBaseTerm)
	data1 := w1.Bytes()
	//fmt.Println(rf.me, "persist")

	rf.persister.SaveStateAndSnapshot(data1, snapshot)
}

func (rf *Raft) readSnapshot(data []byte) []byte {
	//if data == nil || len(data) < 1 { // bootstrap without any state?
	//	return
	//}
	//fmt.Println(rf.me, "data is ok")
	//r := bytes.NewBuffer(data)
	//d := labgob.NewDecoder(r)
	//var snapshot []byte
	//var logBaseIndex int
	//var logBaseTerm int
	//if d.Decode(&snapshot) != nil || d.Decode(&logBaseIndex) != nil || d.Decode(&logBaseTerm) != nil {
	//	fmt.Println("Decode error!")
	//} else {
	//	rf.snapshot = snapshot
	//	rf.logBaseIndex = logBaseIndex
	//	rf.logBaseTerm = logBaseTerm
	//}
	return data
}

func (rf *Raft) leaderSendSnapshot(idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTime = rf.setElectionTime()
	args := &InstallSnapshotArgs{
		Term:             rf.term,
		LastIncludeIndex: rf.logBaseIndex,
		LastIncludeTerm:  rf.logBaseTerm,
		Data:             rf.snapshot,
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	rf.sendSnapshot(idx, args, reply)
	rf.mu.Lock()
	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.state = follow
		rf.voteFor = -1
		return
	}

	//fmt.Println(idx, "Install Snapshot", args.LastIncludeIndex, rf.nextIndex)
	rf.nextIndex[idx] = args.LastIncludeIndex + 1
	rf.matchIndex[idx] = args.LastIncludeIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.state = follow
		rf.vote = false
	}
	reply.Term = rf.term

	if args.Term < rf.term || args.LastIncludeIndex <= rf.logBaseIndex {
		return
	}

	go rf.applySnapshot(args)
}

type InstallSnapshotArgs struct {
	Term             int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
