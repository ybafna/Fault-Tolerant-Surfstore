package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	// fmt.Println("Inside GetBlock")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// fmt.Println("Inside PutBlock")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	if err != nil {
		fmt.Println(err)
		conn.Close()
		return err
	}
	*succ = success.Flag
	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	fmt.Println("Inside HasBlocks")
	// TODO
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// fmt.Println("Inside GetFileInfoMap")
	majorityFailed := 0
	for idx, _ := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err = c.AppendEntries(ctx, &AppendEntryInput{Term: -1})
		if err != nil {
			majorityFailed += 1
			if(majorityFailed > len(surfClient.MetaStoreAddrs)/2){
				return fmt.Errorf("Sync Failed")
			}
			continue
		}
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			// conn.Close()
			return err
		}

		// Check again
		fmt.Println("File Info Map : ", fileInfoMap)
		if fileInfoMap == nil {
			*serverFileInfoMap = nil
		} else {
			*serverFileInfoMap = fileInfoMap.FileInfoMap
		}
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	fmt.Println("Inside UpdateFile")
	fmt.Println("File Name", fileMetaData.GetFilename())

	for idx, _ := range surfClient.MetaStoreAddrs {
		fmt.Println("Host :", surfClient.MetaStoreAddrs[idx])
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithInsecure())
		if err != nil {
			fmt.Println("Could not connect to server :", idx)
			continue
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			fmt.Println(err.Error())
			if err == ERR_SERVER_CRASHED {
				return err
			}
			if err == ERR_NOT_LEADER {
				fmt.Println("Server :", idx, " is not a leader")
			}
			// conn.Close()
			continue
		}
		*latestVersion = version.Version
		break
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// fmt.Println("Inside GetBlockStoreAddr")
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		// conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
