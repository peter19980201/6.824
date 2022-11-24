package raft

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

func (rf *Raft) appendEntries(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx != rf.me {
			if heartbeat == true || rf.log[len(rf.log)-1].Index >= rf.nextIndex[idx] {
				//fmt.Println(rf.me, idx, rf.nextIndex, rf.logBaseIndex)
				if rf.nextIndex[idx] <= rf.logBaseIndex {
					go rf.leaderSendSnapshot(idx)
					continue
				}
				//这里不知道为什么会出现这种情况，先这样重置一下nextIndex
				if rf.logBaseIndex+len(rf.log)-rf.nextIndex[idx] < 0 {
					//fmt.Println(idx, rf.logBaseIndex, len(rf.log), rf.nextIndex[idx])
					rf.nextIndex[idx] = rf.logBaseIndex + len(rf.log)
				}

				args := &AppendEntriesArgs{
					Term:         rf.term,
					LeaderId:     rf.me,
					PreLogIndex:  rf.nextIndex[idx] - 1,
					PreLogTerm:   rf.log[rf.nextIndex[idx]-1-rf.logBaseIndex].Term,
					Entries:      make([]Entry, rf.logBaseIndex+len(rf.log)-rf.nextIndex[idx]),
					LeaderCommit: rf.commitIndex,
				}
				//对于preLog是log中的第一个时，需要特殊处理，将从log中读取换成读取preLog参数
				if rf.nextIndex[idx]-1-rf.logBaseIndex == 0 {
					args.PreLogTerm = rf.logBaseTerm
				}
				copy(args.Entries, rf.log[rf.nextIndex[idx]-rf.logBaseIndex:len(rf.log)])
				//fmt.Println(rf.me, idx, args)
				rf.electionTime = rf.setElectionTime()
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

	//if len(args.Entries) != 0 {
	if reply.Term > args.Term {
		//here need to use args.Term, not rf.term!!!!!!
		rf.state = follow
		rf.term = reply.Term
		rf.voteFor = -1
		rf.vote = false
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
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				rf.commitIndex = rf.matchIndex[idx]
			}
		}
	} else {
		lastLogInTerm := -1
		for i := args.PreLogIndex - rf.logBaseIndex; i > 1; i-- {
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
		//fmt.Println(rf.me, rf.nextIndex, "after a appendEntries")
		//rf.nextIndex[idx]--
		go rf.appendEntries(false)
	}
	//fmt.Println("leaderSendAppendEntries over")
	//} else {
	//	if reply.Term > args.Term {
	//		//fmt.Println(rf.me, "receive big term")
	//		rf.state = follow
	//		rf.term = reply.Term
	//		rf.persist()
	//	}
	//}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	//fmt.Println(rf.me, "reset time")
	rf.electionTime = rf.setElectionTime()
	//fmt.Println("some info", args, rf.logBaseIndex)
	//fmt.Println(rf.me, rf.state, rf.term, rf.log[len(rf.log)-1], rf.commitIndex, rf.nextIndex)
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}

	if args.Term > rf.term {
		reply.Term = rf.term
		rf.toBeFollower(args.Term)
		reply.Success = false
		rf.vote = false
		rf.persist()
		return
	}

	if args.Term == rf.term && rf.state == candidate {
		rf.toBeFollower(args.Term)
		rf.persist()
	}
	reply.Term = args.Term
	rf.voteFor = -1

	if args.PreLogIndex >= rf.logBaseIndex+len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.logBaseIndex + len(rf.log)
		return
	}

	//fmt.Println(rf.me, args.PreLogIndex, rf.log[len(rf.log)-1], rf.commitIndex)
	if args.PreLogIndex < rf.logBaseIndex {
		//fmt.Println(rf.me, "error!", args.PreLogIndex, rf.logBaseIndex, rf.commitIndex)
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.logBaseIndex + len(rf.log)
		return
	}
	//对于preLog是log中的第一个时，需要特殊处理，将从log中读取换成读取preLog参数
	tmpPreLogTerm := 0
	if args.PreLogIndex-rf.logBaseIndex == 0 {
		tmpPreLogTerm = rf.logBaseTerm
	} else {
		tmpPreLogTerm = rf.log[args.PreLogIndex-rf.logBaseIndex].Term
	}

	if args.PreLogTerm != tmpPreLogTerm {
		reply.Success = false
		reply.ConflictTerm = tmpPreLogTerm
		for i := 0; i <= args.PreLogIndex-rf.logBaseIndex; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		//fmt.Println("333333333333333333333", args.PreLogTerm, args.PreLogIndex, rf.logBaseIndex, reply.ConflictIndex)
		return
	}

	for idx, entry := range args.Entries {
		if entry.Index < rf.logBaseIndex+len(rf.log) && rf.log[entry.Index-rf.logBaseIndex].Term != entry.Term {
			rf.log = append(rf.log[:entry.Index-rf.logBaseIndex], args.Entries[idx:]...)
			break
		}
		if entry.Index >= rf.logBaseIndex+len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	rf.persist()
	//fmt.Println(rf.me, "change my commit", args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Println(rf.me, args.PreLogIndex, "trouble!")
		if len(rf.log) == 1 {
			rf.commitIndex = min(args.LeaderCommit, rf.logBaseTerm)
		} else {
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
	}
}
