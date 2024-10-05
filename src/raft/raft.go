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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

type Entry struct {
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	log            []Entry
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	state          State
	lastBeatenTime time.Time
	voteNumber     int
	isGettingVotes bool
	appendCond []*sync.Cond
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) becomeLeader() {
	DPrintf("Server %d becomes leader! (log: %v)\n", rf.me, rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.heartbeatTicker()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.appendCond[i] = sync.NewCond(&rf.mu)
			go rf.appendWaiter(i)
		}
	}
	go rf.commitTicker()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) updateCurrentTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		if rf.state == Leader {
			DPrintf("Server %d is not the leader\n", rf.me)
		}
		rf.state = Follower
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// DPrintf("request vote %d ==> %d\n" ,args.CandidateId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	rf.updateCurrentTerm(args.Term)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		myLastIndex := len(rf.log) - 1
		myLastTerm := rf.log[myLastIndex].Term
		if args.LastLogTerm > myLastTerm || args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex {
			DPrintf("Server %d vote for %d (term = %d)\n", rf.me, args.CandidateId, rf.currentTerm)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastBeatenTime = time.Now()
			return
		}
	}
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Server %d] Log: %d", rf.me, rf.log)
	DPrintf("[Server %d] handle appendentries prev(index: %d, term: %d) entries: %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	rf.lastBeatenTime = time.Now()
	reply.Term = rf.currentTerm
	if rf.state == Candidate {
		rf.state = Follower
		rf.isGettingVotes = false
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.updateCurrentTerm(args.Term)
	}
	if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
	} else {
		reply.Success = false
	}
	DPrintf("[Server %d] handle appendentries result: %v", rf.me, reply.Success)
	if reply.Success {
		index := args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			if index >= len(rf.log) {
				break
			}
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[: index]
				break
			}
			index++
		}
		index = args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			if index >= len(rf.log) {
				rf.log = append(rf.log, entry)
				// DPrintf("[Server %d] append %v", rf.me, entry)
			}
			index++
		}
		DPrintf("[Server %d] commitindex = %d\n", rf.me, rf.commitIndex)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
			DPrintf("[Server %d] commitindex => %d", rf.me, rf.commitIndex)
			rf.applyCond.Signal()
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	_, isLeader := rf.GetState()

	// Your code here (3B).
	// DPrintf("[Leader] client send command %v\n", command)
	if !isLeader {
		return index, term, isLeader
	}
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, Entry{Term: term, Command: command})
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId != rf.me {
			rf.appendCond[peerId].Signal()
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) constructRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	return args
}

func (rf *Raft) handleRequestVoteReply(ok bool, target int, reply RequestVoteReply) {
	if !ok {
		return
	}
	// DPrintf("%d request vote get %t\n", rf.me, reply.VoteGranted)
	rf.updateCurrentTerm(reply.Term)
	if rf.isGettingVotes && reply.VoteGranted {
		rf.voteNumber++
		DPrintf("server %d get vote from Server %d (%d votes)\n", rf.me, target, rf.voteNumber)
		if rf.voteNumber > len(rf.peers)/2 {
			rf.isGettingVotes = false
			rf.state = Leader
			rf.becomeLeader()
		}
	}

}

func (rf *Raft) startElection() {
	DPrintf("server %d start election\n", rf.me)
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteNumber = 1
	rf.lastBeatenTime = time.Now()
	rf.isGettingVotes = true
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId != rf.me {
			go func(peerId int) {
				rf.mu.Lock()
				args := rf.constructRequestVoteArgs()
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerId, &args, &reply)
				rf.mu.Lock()
				rf.handleRequestVoteReply(ok, peerId, reply)
				rf.mu.Unlock()
			}(peerId)
		}
	}
}

func (rf *Raft) ticker() {
	ms := 500 + (rand.Int63() % 500)
	lastSleepTime := time.Now().Add(-time.Duration(ms) * time.Millisecond)
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// DPrintf("[Server %d] check condition %v %v\n", rf.me, rf.lastBeatenTime, lastSleepTime)
		
		// DPrintf("[server %d lock]2\n",rf.me)
		_, isLeader := rf.GetState()
		rf.mu.Lock()
		if rf.lastBeatenTime.Before(lastSleepTime) && !isLeader {
			// DPrintf("[Server %d] check over %v %v\n", rf.me, rf.lastBeatenTime, lastSleepTime)
			rf.startElection()
		}
		rf.mu.Unlock()
		// DPrintf("[server %d unlock]2\n",rf.me)

		// pause for a random amount of time between 500 and 1000
		// milliseconds.
		ms := 500 + (rand.Int63() % 500)
		lastSleepTime = time.Now()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) constructAppendEntriesArgs(target int, entries []Entry) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[target] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = entries
	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) handleAppendEntriesReply(ok bool, target int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !ok {
		return
	}
	DPrintf("handle appendentries from server %d: reply: %v", target, reply)
	rf.updateCurrentTerm(reply.Term)
	if reply.Success {
		rf.matchIndex[target] = args.PrevLogIndex + len(args.Entries)
		DPrintf("[Leader] Server %d's matchindex become %d + %d = %d\n", target, args.PrevLogIndex, len(args.Entries), rf.matchIndex[target])
		rf.nextIndex[target] = rf.matchIndex[target] + 1
	} else {
		rf.nextIndex[target]--
	}
	DPrintf("[Leader] Server %d's nextindex is %d\n",target, rf.nextIndex[target])
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			break
		}
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId != rf.me {
				go func(peerId int) {
					rf.mu.Lock()
					args := rf.constructAppendEntriesArgs(peerId, rf.log[rf.nextIndex[peerId]:])
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					DPrintf("[Leader] send heartbeat to server %d\n", peerId)
					ok := rf.sendAppendEntries(peerId, &args, &reply)
					rf.mu.Lock()
					rf.handleAppendEntriesReply(ok, peerId, args, reply)
					rf.mu.Unlock()
				}(peerId)
			}
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

func (rf *Raft) appendWaiter(target int) {
	for !rf.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			break
		}
		rf.mu.Lock()
		for rf.nextIndex[target] >= len(rf.log) {
			rf.appendCond[target].Wait()
		}
		args := rf.constructAppendEntriesArgs(target, rf.log[rf.nextIndex[target]:])
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(target, &args, &reply)
		rf.mu.Lock()
		rf.handleAppendEntriesReply(ok, target, args, reply)
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitTicker() {
	for !rf.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			break
		}
		rf.mu.Lock()
		for {
			num := 1
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId != rf.me {
					if rf.matchIndex[peerId] > rf.commitIndex {
						num++
					}
				}
			}
			if num > len(rf.peers) / 2 {
				rf.commitIndex++
				rf.applyCond.Signal()
			} else {
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) applyWaiter() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			// DPrintf("[lock]1\n")
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		DPrintf("[Server %d (state:%v)] %v apply to state machine\n", rf.me, rf.state, ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied})
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.isGettingVotes = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendCond = make([]*sync.Cond, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyWaiter()

	return rf
}
