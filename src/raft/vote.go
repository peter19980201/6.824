package raft

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
	reply.Vote = false
	// upToData:compare term first (need big or equal), if ok, compare index (need big or equal)
	// if index is smaller, but term is bigger, is ok
	var upToData bool
	if len(rf.log) == 1 {
		upToData = args.LastLogTerm > rf.logBaseTerm
		upToData = upToData || (args.LastLogTerm == rf.logBaseTerm && args.LastLogIndex >= rf.logBaseTerm)
	} else {
		upToData = args.LastLogTerm > rf.log[len(rf.log)-1].Term
		upToData = upToData || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)
	}

	if args.Term > rf.term {
		rf.toBeFollower(args.Term)
	}
	if args.Term == rf.term {
		if upToData {
			if rf.voteFor == -1 {
				reply.Vote = true
				rf.voteFor = args.Candidate
				rf.state = follow
				rf.persist()
				rf.electionTime = rf.setElectionTime()
			}
		}
	}

	// Your code here (2A, 2B).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	if len(rf.log) == 1 {
		args.LastLogTerm = rf.logBaseTerm
		args.LastLogIndex = rf.logBaseIndex
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
			rf.toBeFollower(reply.Term)
			rf.persist()
			return
		} else if reply.Term <= args.Term {
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

func (rf *Raft) toBeFollower(term int) {
	rf.term = term
	rf.state = follow
	rf.voteFor = -1
}
