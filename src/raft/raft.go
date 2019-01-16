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

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "sync/atomic"
import "sort"

import "bytes"
import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type state int
const (
	Follower state = 1
	Leader state = 2
	Candidate state = 3
)

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state state
	heartbeatTimeout time.Duration
	appendEntryCh chan bool
	voteCh chan bool
	leaderCh chan bool
	applyCh chan ApplyMsg

	// Persistent for all servers
	currentTerm int
	votedFor int
	log []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	
	// Volatile state on leaders:
	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without state.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { 
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() || args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		receiveThenAssign(rf.voteCh, true)
		return
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, false
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	args := RequestVoteArgs { 
		Term: rf.currentTerm, 
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var voteNum int32 = 1
	for index, _ := range rf.peers {
		if index == rf.me { continue }
		go func(index int, args RequestVoteArgs){
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(index, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				
				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.convert2Follower(reply.Term)
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&voteNum, 1)
				}
				if atomic.LoadInt32(&voteNum) > int32(len(rf.peers) / 2) {
					rf.convert2Leader()
					receiveThenAssign(rf.leaderCh, true)
				}
			}
		}(index, args)
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	// for optimization
	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term, reply.Success = rf.currentTerm, false
	reply.ConflictIndex, reply.ConflictTerm = 0, 0

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	receiveThenAssign(rf.appendEntryCh, true)

	// log doesn’t contain an entry at prevLogIndex
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
	} else { // log contain an entry at prevLogIndex, but there might be conflicts.
		prevLogTerm := rf.log[args.PrevLogIndex].Term
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if prevLogTerm != args.PrevLogTerm {
			reply.ConflictTerm = prevLogTerm
			for i := 1; i < len(rf.log); i++ {
				if rf.log[i].Term == prevLogTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}

		if prevLogTerm == args.PrevLogTerm || args.PrevLogIndex == 0 {
			// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			// Append any new entries not already in the log
			for i, entry := range args.Entries {
				index := args.PrevLogIndex + 1 + i
				if index > rf.getLastLogIndex() {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
				if rf.log[index].Term != args.Term {
					rf.log = rf.log[0 : index]
				}
				rf.log = append(rf.log, entry)
			}

			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit > rf.getLastLogIndex() {
					rf.commitIndex = rf.getLastLogIndex()
				} else {
					rf.commitIndex = args.LeaderCommit
				}
			}
			reply.Success = true
		}
	}
	rf.applyLogs()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startAppendEntries() {
	for index, _ := range rf.peers {
		if index == rf.me { continue }
		go func(index int){
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				nextIndex := rf.nextIndex[index]
				entries := append(make([]LogEntry, 0), rf.log[nextIndex:]...)
				args := AppendEntriesArgs{ 
					Term: rf.currentTerm, 
					LeaderId: rf.me,
					PrevLogIndex: rf.getPrevLogIndex(index),
					PrevLogTerm: rf.getPrevLogTerm(index),
					Entries: entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
	
				if rf.sendAppendEntries(index, args, reply) {
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.convert2Follower(reply.Term)
						rf.mu.Unlock()
						return
					}
					if !(rf.state == Leader && rf.currentTerm == args.Term) {
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[index] = rf.matchIndex[index] + 1

						// If there exists an N such that N > commitIndex, a majority
						// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
						// set commitIndex = N (§5.3, §5.4).
						sortedMatchIndex := make([]int, len(rf.matchIndex))
						copy(sortedMatchIndex, rf.matchIndex)
						sortedMatchIndex[rf.me] = len(rf.log) - 1
						sort.Ints(sortedMatchIndex)
						N := sortedMatchIndex[len(rf.peers)/2]
						// DPrintf("matchIndexes:%v, N:%v", sortedMatchIndex, N)
						if rf.state == Leader && N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							rf.applyLogs()
						}
						rf.mu.Unlock()
						return
					} else {
						newIndex := reply.ConflictIndex
						for i, entry := range rf.log {
							if entry.Term == reply.ConflictTerm {
								newIndex = i + 1
							}
						}
						if newIndex > 1 {
							rf.nextIndex[index] = newIndex
						} else {
							rf.nextIndex[index] = 1
						}
						rf.mu.Unlock()
					}
				} else {
					return
				}
			}
		}(index)
	}
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		// DPrintf("Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v, %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Command, rf.state)
		rf.lastApplied++
		msg := ApplyMsg{
			Index: rf.log[rf.lastApplied].Index,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.applyCh <- msg
	}
}

func receiveThenAssign(ch chan bool, value bool) {
	select {
	case <-ch:
	default:
	}
	ch <- value
}

func (rf *Raft) convert2Candidate() {
	defer rf.persist()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) convert2Follower(term int) {
	defer rf.persist()
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) convert2Leader() {
	// defer rf.persist()
	if rf.state != Candidate {
		return
	}
	rf.state = Leader

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	rf.nextIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastLogIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.log[lastLogIndex].Term
	}
}

func (rf *Raft) getPrevLogIndex(index int) int {
	return rf.nextIndex[index] - 1
}

func (rf *Raft) getPrevLogTerm(index int) int {
	prevLogIndex := rf.getPrevLogIndex(index)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{
			Term: term,
			Index: index,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.heartbeatTimeout = 50 * time.Millisecond // 50ms
	rf.appendEntryCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = append(make([]LogEntry, 0), LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for{
			electionTimeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond // 150ms - 300ms
			rf.mu.Lock()
			state := rf.state
			DPrintf("%v,%v,%v,%v,%v", rf.me, rf.log, rf.commitIndex, rf.lastApplied, rf.state==Leader)
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				case <-rf.appendEntryCh:
				case <-rf.voteCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convert2Candidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(rf.heartbeatTimeout)
			case Candidate:
				go rf.election()
				select {
				case <-rf.appendEntryCh:
				case <-rf.voteCh:
				case <-rf.leaderCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convert2Candidate()
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}
