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
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

const (
	leader = iota
	candidate
	follow
)

//
// A Go object implementing a single Raft peer.
//
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term         int
	state        int
	heartBeat    time.Duration
	electionTime time.Time
	voteFor      int

	log         []Entry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	logBaseIndex int
	logBaseTerm  int
	snapshot     []byte

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	if rf.state == leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.log)
	e.Encode(rf.voteFor)
	data := w.Bytes()
	//fmt.Println(rf.me, "persist")
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var log []Entry
	var voteFor int
	if d.Decode(&term) != nil || d.Decode(&log) != nil || d.Decode(&voteFor) != nil {
		fmt.Println("Decode error!")
	} else {
		//fmt.Println("restart:", term, log, voteFor)
		rf.term = term
		rf.log = log
		rf.voteFor = voteFor
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex <= rf.log[len(rf.log)-1].Index {
		rf.log = append([]Entry(nil), rf.log[lastIncludedIndex-rf.logBaseIndex:]...)
	} else {
		rf.log = append([]Entry(nil), Entry{-1, 0, 0})
	}

	rf.logBaseIndex = lastIncludedIndex
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.saveStateAndSnapshot()

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
	if index <= rf.logBaseIndex {
		return
	}
	rf.log = rf.log[index-rf.logBaseIndex:]
	rf.logBaseIndex = index
	rf.snapshot = snapshot

	rf.saveStateAndSnapshot()
}

func (rf *Raft) saveStateAndSnapshot() {
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.term)
	e1.Encode(rf.log)
	e1.Encode(rf.voteFor)
	data1 := w1.Bytes()
	//fmt.Println(rf.me, "persist")

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(rf.snapshot)
	data2 := w2.Bytes()

	rf.persister.SaveStateAndSnapshot(data1, data2)
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot []byte
	if d.Decode(&snapshot) != nil {
		fmt.Println("Decode error!")
	} else {
		//fmt.Println("restart:", term, log, voteFor)
		rf.snapshot = snapshot
	}
}

func (rf *Raft) leaderSendSnapshot(idx int) {
	rf.mu.Lock()
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
	defer rf.mu.Unlock()

	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.state = follow
		rf.voteFor = -1
		return
	}

	rf.nextIndex[idx] = args.LastIncludeIndex + 1
	rf.matchIndex[idx] = args.LastIncludeIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTime = rf.setElectionTime()

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.state = follow
	}
	reply.Term = rf.term

	if args.Term < rf.term || args.LastIncludeIndex <= rf.logBaseIndex {
		return
	}

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludeTerm,
	}
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term      int
	Candidate int

	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Vote bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	// upToData:compare term first (need big or equal), if ok, compare index (need big or equal)
	// if index is smaller, but term is bigger, is ok
	upToData := args.LastLogTerm > rf.log[len(rf.log)-1].Term
	upToData = upToData || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)
	if args.Term < rf.term {
		reply.Vote = false
	} else if args.Term == rf.term {
		if upToData {
			if rf.voteFor == args.Candidate || rf.voteFor == -1 {
				reply.Vote = true
				rf.voteFor = args.Candidate
				rf.state = follow
				rf.persist()
			}
		} else {
			reply.Vote = false
		}
	} else if args.Term > rf.term {
		if upToData {
			reply.Vote = true
			rf.voteFor = args.Candidate
			rf.term = args.Term
			reply.Term = rf.term
			rf.state = follow
			rf.persist()
		} else {
			rf.term = args.Term
			reply.Term = rf.term
			rf.persist()
			reply.Vote = false
		}
	}

	rf.electionTime = rf.setElectionTime()
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer func() {
	//	if reply.Success == false && reply.ConflictIndex == 0 {
	//		fmt.Println("follower here", rf.me, rf.term)
	//	}
	//}()
	//fmt.Println(rf.me, "receive heartbeat from", args)
	reply.Success = true
	rf.electionTime = rf.setElectionTime()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}

	if args.Term > rf.term {
		reply.Term = rf.term
		rf.term = args.Term
		rf.state = follow
		rf.voteFor = -1
		reply.Success = false
		rf.persist()
		return
	}

	reply.Term = args.Term

	if args.PreLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}

	if args.PreLogTerm != rf.log[args.PreLogIndex].Term {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PreLogIndex].Term
		for i := 1; i <= args.PreLogIndex; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	for idx, entry := range args.Entries {
		if entry.Index < len(rf.log) && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		}
		if entry.Index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		//fmt.Println(rf.me, args.PreLogIndex, "trouble!")
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return index, rf.term, false
	}
	log := Entry{
		Command: command,
		Term:    rf.term,
		Index:   len(rf.log),
	}

	rf.log = append(rf.log, log)
	//fmt.Println(rf.me, "new Entry coming...", command)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.persist()
	go rf.appendEntries(false)

	index = len(rf.log) - 1
	term = rf.term
	isLeader = true

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		//fmt.Println(rf.me, rf.log, rf.commitIndex)
		//fmt.Println(rf.me, "state:", rf.state, "term:", rf.term, "voteFor:", rf.voteFor)
		if rf.state == leader {
			rf.electionTime = rf.setElectionTime()
			go rf.appendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			//fmt.Println(rf.me, "time out! become candidate!")
			go rf.leaderElection()
		}
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) appendEntries(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx != rf.me {
			if heartbeat == true || rf.log[len(rf.log)-1].Index > rf.nextIndex[idx] {
				args := &AppendEntriesArgs{
					Term:         rf.term,
					LeaderId:     rf.me,
					PreLogIndex:  rf.nextIndex[idx] - 1,
					PreLogTerm:   rf.log[rf.nextIndex[idx]-1].Term,
					Entries:      make([]Entry, len(rf.log)-rf.nextIndex[idx]),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.log[rf.nextIndex[idx]:len(rf.log)])
				go rf.leaderSendEntries(idx, args)
			}
		}
	}
}

func (rf *Raft) leaderSendEntries(idx int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	//fmt.Println(rf.me, "send heartbeat", idx)
	ok := rf.sendAppendEntries(idx, args, reply)
	if ok == false {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(reply)
	if len(args.Entries) != 0 {
		if reply.Term > args.Term {
			//here need to use args.Term, not rf.term!!!!!!
			rf.state = follow
			rf.term = reply.Term
			rf.persist()
			return
		} else if reply.Term < args.Term {
			return
		}

		if args.PreLogIndex != rf.nextIndex[idx]-1 {
			return
		}

		if reply.Success == true {
			//fmt.Println(rf.me, args.PreLogIndex, args.Entries)
			rf.nextIndex[idx] = args.PreLogIndex + len(args.Entries) + 1
			rf.matchIndex[idx] = args.PreLogIndex + len(args.Entries)
			count := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= rf.matchIndex[idx] {
					count++
				}
			}
			//fmt.Println("start compare count and totalNum", count)
			if count > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[idx] && rf.term == args.Term {
				rf.commitIndex = rf.matchIndex[idx]
			}
		} else {
			if reply.ConflictIndex == 0 {
				fmt.Println("here find a 0!", reply)
			}
			lastLogInTerm := -1
			for i := args.PreLogIndex; i > 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastLogInTerm = i
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}
			if lastLogInTerm == -1 {
				rf.nextIndex[idx] = reply.ConflictIndex
			} else {
				rf.nextIndex[idx] = lastLogInTerm
			}
			//rf.nextIndex[idx]--
			go rf.appendEntries(false)
		}
		//fmt.Println("leaderSendAppendEntries over")
	} else {
		if reply.Term > rf.term {
			//fmt.Println(rf.me, "receive big term")
			rf.state = follow
			rf.term = reply.Term
			rf.persist()
		}
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.term++
	rf.state = candidate
	rf.voteFor = rf.me
	rf.persist()
	rf.electionTime = rf.setElectionTime()

	args := &RequestVoteArgs{
		Term:         rf.term,
		Candidate:    rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	voteCount := 1
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go rf.candidateRequestVote(idx, args, &voteCount)
		}
	}
}

func (rf *Raft) candidateRequestVote(idx int, args *RequestVoteArgs, voteCount *int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(idx, args, reply)
	if ok == false {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == candidate {
		if reply.Term > args.Term {
			rf.term = reply.Term
			rf.voteFor = -1
			rf.state = follow
			rf.persist()
			return
		} else if reply.Term == args.Term {
			if reply.Vote == true {
				*voteCount++
			} else {
				return
			}
		}

		if *voteCount > len(rf.peers)/2 {
			//fmt.Println(rf.me, "become leader~!")
			rf.state = leader
			rf.voteFor = rf.me
			rf.persist()
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		term:        0,
		state:       follow,
		heartBeat:   100 * time.Millisecond,
		voteFor:     -1,
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTime = rf.setElectionTime()
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{-1, 0, 0})
	rf.nextIndex = make([]int, len(peers))
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			rf.mu.Unlock()
			//fmt.Println(rf.me, applyMsg)
			rf.applyCh <- applyMsg
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		}
	}
}

func (rf *Raft) setElectionTime() time.Time {
	return time.Now().Add(time.Duration(150+rand2.Int()%150) * time.Millisecond)
}
