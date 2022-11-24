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
	CommandTerm  int

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
	vote         bool

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
	e.Encode(rf.logBaseIndex)
	e.Encode(rf.logBaseTerm)
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
	var logBaseIndex int
	var logBaseTerm int
	if d.Decode(&term) != nil || d.Decode(&log) != nil || d.Decode(&voteFor) != nil || d.Decode(&logBaseIndex) != nil || d.Decode(&logBaseTerm) != nil {
		fmt.Println("Decode error!")
	} else {
		//fmt.Println("restart:", term, log, voteFor)
		rf.term = term
		rf.log = log
		rf.voteFor = voteFor
		rf.logBaseTerm = logBaseTerm
		rf.logBaseIndex = logBaseIndex
		//fmt.Println("restart: logBaseIndex, logBaseTerm", rf.logBaseIndex, rf.logBaseTerm)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

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
		Index:   rf.logBaseIndex + len(rf.log),
	}

	rf.log = append(rf.log, log)
	//fmt.Println(rf.me, "new Entry coming...", command)
	rf.matchIndex[rf.me] = rf.logBaseIndex + len(rf.log) - 1
	rf.persist()
	go rf.appendEntries(false)

	index = rf.logBaseIndex + len(rf.log) - 1
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
		if len(rf.log) > 1 {
			//fmt.Println(rf.me, rf.log[1], rf.log[len(rf.log)-1], rf.commitIndex, rf.nextIndex, "state:", rf.state, "term:", rf.term, "voteFor:", rf.voteFor)
		}

		if rf.state == leader {
			go rf.appendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			//fmt.Println(rf.me, "time out! become candidate!", rf.term+1)
			go rf.leaderElection()
		}
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

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

	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.readSnapshot(persister.ReadSnapshot())
	rf.lastApplied = rf.logBaseIndex
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.log) + rf.logBaseIndex
	}

	args := &InstallSnapshotArgs{
		LastIncludeIndex: rf.logBaseIndex,
		LastIncludeTerm:  rf.logBaseTerm,
		Data:             rf.snapshot,
	}
	go rf.applySnapshot(args)

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
				Command:      rf.log[rf.lastApplied-rf.logBaseIndex].Command,
				CommandIndex: rf.log[rf.lastApplied-rf.logBaseIndex].Index,
				CommandTerm:  rf.log[rf.lastApplied-rf.logBaseIndex].Term,
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

func (rf *Raft) applySnapshot(args *InstallSnapshotArgs) {
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludeTerm,
	}
}

func (rf *Raft) setElectionTime() time.Time {
	return time.Now().Add(time.Duration(150+rand2.Int()%150) * time.Millisecond)
}

func (rf *Raft) SizeOfRaftLog() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ans := rf.persister.RaftStateSize()

	return ans
}
