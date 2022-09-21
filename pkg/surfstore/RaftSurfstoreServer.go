package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	// Server Info
	ip          string
	ipList      []string
	serverId    int64
	lastApplied int64

	//Follower info only for leaders
	nextIndex  []int64
	matchIndex []int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{
		FileInfoMap: s.metaStore.FileMetaMap,
	}, nil

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	fmt.Println("New changes:", s.log)
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)
	fmt.Println("Pending Commits", s.pendingCommits)
	success := <-committed
	if success {
		fmt.Println("File :", op.FileMetaData.Filename, " committed to server")
		// s.pendingCommits = s.pendingCommits[:len(s.pendingCommits)-1]
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) commitWorker() {
	for {
		// TODO check all state, don't run if crashed, not the leader

		if !s.isLeader || s.commitIndex == int64(len(s.log)-1) {
			continue
		}

		if s.isCrashed {
			return
		}

		s.lastApplied = int64(len(s.log) - 1)
		targetIdx := s.commitIndex + 1
		commitChan := make(chan *AppendEntryOutput, len(s.ipList))
		for idx, _ := range s.ipList {
			if s.ipList[idx] == s.ip {
				continue
			}

			go s.commitEntry(int64(idx), targetIdx, commitChan)
		}

		commitCount := 1
		for {
			commit := <-commitChan
			if commit != nil && commit.Success {
				commitCount++
			}
			fmt.Println("Commit Count :", commitCount)
			if commitCount > len(s.ipList)/2 {
				s.pendingCommits[targetIdx] <- true
				s.commitIndex = targetIdx
				fmt.Println("Server", s.serverId, " committed till :", targetIdx)
				break
			}
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		// TODO set up clients or call grpc.Dial
		// client := s.rpcClients[serverIdx]

		if s.isCrashed {
			commitChan <- &AppendEntryOutput{Success: false}
			return
		}

		fmt.Println("Dial to:", serverIdx)
		conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithInsecure())
		if err != nil {
			log.Fatal("Error connecting to clients ", err)
		}
		client := NewRaftSurfstoreClient(conn)

		fmt.Println("Current Term:", s.term, " Entry Idx :", entryIdx)
		// TODO create correct AppendEntryInput from s.nextIndex, etc
		var input *AppendEntryInput
		if entryIdx != -1 {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: entryIdx,
				PrevLogTerm:  s.log[entryIdx].Term,
				LeaderCommit: s.commitIndex,
				Entries:      s.log[entryIdx+1:],
			}
		} else {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: entryIdx,
				// PrevLogTerm:  s.log[entryIdx].Term,
				LeaderCommit: s.commitIndex,
				Entries:      s.log,
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)

		if err != nil {
			fmt.Println("Client with id ", serverIdx, " is crashed")
			// return
			continue
		} else if output.Success {
			// TODO update state. s.nextIndex, etc
			s.nextIndex[serverIdx] = int64(len(s.log) - 1)
			commitChan <- output
			return
		} else {
			s.nextIndex[serverIdx] -= 1
			entryIdx -= 1
			// if entryIdx == -2 {
			// 	commitChan <- output
			// 	return
			// }
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	fmt.Println("Input Term:", input.Term, " Client Term:", s.term, "Logs:", s.log)
	fmt.Println("PrevLogIndex :", input.PrevLogIndex, "PrevLogTerm :", input.PrevLogTerm)
	if input.Term < s.term || input.PrevLogIndex+1 > int64(len(s.log)) {
		fmt.Println("Case 1 or Case 2 not satisfied")
		return &AppendEntryOutput{Success: false}, nil
	}

	s.term = input.Term
	s.isLeader = false
	// if (input.Term == 1 && input.PrevLogIndex != 0) && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
	if input.PrevLogIndex != -1 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		// Delete the existing entry and all that follow it
		fmt.Println("Case 3")
		s.log = s.log[:input.PrevLogIndex]
		fmt.Println("Updated Logs:", s.log)
	}

// 	if(len(s.log) < len(input.Entries)){
		for _, entry := range input.Entries {
			fmt.Println("Appending New Entries to logs")
			s.log = append(s.log, entry)
			s.metaStore.FileMetaMap[entry.FileMetaData.Filename] = entry.FileMetaData
			fmt.Println("Meta Map updated to:")
			PrintMetaMap(s.metaStore.FileMetaMap)
			s.lastApplied += 1
		}
// 	}

	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit < input.PrevLogIndex+1 {
			fmt.Println("Inside If")
			s.commitIndex = input.LeaderCommit
		} else {
			fmt.Println("Inside Else")
			s.commitIndex = input.PrevLogIndex + 1
		}
		fmt.Println("Client", s.serverId, "committed till :", s.commitIndex)
	}

	fmt.Println("Log appended to server :", s.serverId)

	//TODO
	return &AppendEntryOutput{
		Term:     s.term,
		ServerId: s.serverId,
		Success:  true,
	}, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	fmt.Println("Set Leader Invoked")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.term = s.term + 1
	s.isLeader = true

	// Added to avoid index mismatch when leader changes and target index is being set to s.commitIndex + 1
	s.pendingCommits = make([]chan bool, s.commitIndex+1)
	for i := 0; i < int(s.commitIndex+1); i++ {
		s.pendingCommits[i] = make(chan bool)
	}

	s.nextIndex = make([]int64, len(s.ipList))

	for i := range s.ipList {
		if i == int(s.serverId) {
			continue
		}
		s.nextIndex[i] = s.lastApplied + 1
	}
	fmt.Println("New Leader Set with Id", s.serverId, " for term", s.term)
	fmt.Print("Next Index : ", s.nextIndex)
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	// return nil, nil
	fmt.Println("Heartbeat Invoked for server:", s.serverId)
	if s.isLeader {
		for idx, _ := range s.ipList {
			if idx != int(s.serverId) {
				conn, err := grpc.Dial(s.ipList[idx], grpc.WithInsecure())
				if err != nil {
					log.Fatal("Error connecting to clients ", err)
				}
				client := NewRaftSurfstoreClient(conn)

				var input *AppendEntryInput
				fmt.Println("Current Term:", s.term)
				if len(s.log) == 0 {
					input = &AppendEntryInput{
						Term:         s.term,
						PrevLogIndex: s.lastApplied,
						// PrevLogTerm:  s.log[s.lastApplied].Term,
						LeaderCommit: s.commitIndex,
					}
				} else {
					input = &AppendEntryInput{
						Term:         s.term,
						PrevLogIndex: s.lastApplied,
						PrevLogTerm:  s.log[s.lastApplied].Term,
						LeaderCommit: s.commitIndex,
// 						Entries: s.log[s.nextIndex[idx]:],
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				output, err := client.AppendEntries(ctx, input)
				if err != nil {
					fmt.Println("Client with id ", idx, " is crashed")
					continue
				} else if output.Success {
					// TODO
					continue
				} else {
					s.nextIndex[idx] -= 1
				}
			}
		}

		return &Success{Flag: true}, nil
	} else {
		return &Success{Flag: false}, nil
	}

}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Server with id :", s.serverId, "crashed")
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
