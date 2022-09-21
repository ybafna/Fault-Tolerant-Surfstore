package surfstore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	// fmt.Println("Inside Server's Get Block")
	if val, ok := bs.BlockMap[blockHash.GetHash()]; ok {
		return &Block{
			BlockData: val.GetBlockData(),
			BlockSize: val.GetBlockSize(),
		}, nil
	}

	return nil, errors.New("Not a valid hash key")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashKey := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hashKey] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hasBlocks := &BlockHashes{
		Hashes: make([]string, 0),
	}
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			hasBlocks.Hashes = append(hasBlocks.Hashes, hash)
		}
	}
	return hasBlocks, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
