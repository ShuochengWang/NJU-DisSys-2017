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
import "fmt"
import "math/rand"
import "time"
import "sync/atomic"

// import "bytes"
// import "encoding/gob"



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

	// Persistent for all servers
	currentTerm int
	votedFor int
	log []string

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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
	
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}
	// todo: If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
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
	args := RequestVoteArgs { Term: rf.currentTerm, CandidateId: rf.me }
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
	// entries[]
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}
	// todo: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// todo: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// todo: Append any new entries not already in the log
	// todo: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.state = Follower
	receiveThenAssign(rf.appendEntryCh, true)
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartbeat() {
	for index, _ := range rf.peers {
		if index == rf.me { continue }
		go func(index int){
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{ Term: rf.currentTerm, LeaderId: rf.me}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			if rf.sendAppendEntries(index, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convert2Follower(reply.Term)
					return
				}
			}
		}(index)
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
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) convert2Follower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) convert2Leader() {
	if rf.state != Candidate {
		fmt.Printf("convert2Leader error. Current state is %d", rf.state)
		return
	}
	rf.state = Leader
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
	index := -1
	term := -1
	isLeader := true


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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for{
			electionTimeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond // 150ms - 300ms
			rf.mu.Lock()
			state := rf.state
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
				rf.heartbeat()
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
