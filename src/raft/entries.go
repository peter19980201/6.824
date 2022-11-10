package raft

import "fmt"

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
