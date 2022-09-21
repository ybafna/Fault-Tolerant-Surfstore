package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestSyncTwoClientsFileUpdateLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. leader change. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//client1 syncs
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	content1, _ := ioutil.ReadFile(workingDir + "/test0/multi_file1.txt")
	fmt.Println("TEST 0 Content : ", string(content1))

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	content2, _ := ioutil.ReadFile(workingDir + "/test1/multi_file1.txt")
	fmt.Println("TEST 1 Content : ", string(content2))
}

func TestRaftServerIsCrashable(t *testing.T) {
	t.Logf("a request is sent to a crashed server")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	//client1 syncs
	_, err := test.Clients[0].UpdateFile(context.Background(), filemeta1)

	if err != nil {
		fmt.Println(err.Error())
	}

}

func TestRaftRecoverable(t *testing.T) {
	t.Logf("leader1 gets a request while all other nodes are crashed. the crashed nodes recover.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// worker1 := InitDirectoryWorker("test0", SRC_PATH)
	// worker2 := InitDirectoryWorker("test1", SRC_PATH)
	// worker3 := InitDirectoryWorker("test2", SRC_PATH)
	// defer worker1.CleanUp()
	// defer worker2.CleanUp()
	// defer worker3.CleanUp()

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	go test.Clients[0].UpdateFile(context.Background(), filemeta1)
	time.Sleep(time.Second)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	time.Sleep(2 * time.Second)

	fileMeta1, err := test.Clients[0].GetFileInfoMap(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1.GetFileInfoMap()) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1.GetFileInfoMap()["testFile1"].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	fileMeta2, err := test.Clients[1].GetFileInfoMap(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2.GetFileInfoMap()) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2.GetFileInfoMap()["testFile1"].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	fileMeta3, err := test.Clients[2].GetFileInfoMap(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Could not load meta file for client3")
	}
	if len(fileMeta3.GetFileInfoMap()) != 1 {
		t.Fatalf("Wrong number of entries in client3 meta file")
	}
	if fileMeta3.GetFileInfoMap()["testFile1"].Version != 1 {
		t.Fatalf("Wrong version for file1 in client3 metadata.")
	}

}
