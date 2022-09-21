package surfstore

import (
	context "context"
	"errors"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fmt.Println("Inside Server's Get File Info Map")
	PrintMetaMap(m.FileMetaMap)
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fmt.Println("Inside Server's Update File")
	if m.FileMetaMap == nil {
		return nil, errors.New("No file meta map found")
	}

	var version int32
	if _, ok := m.FileMetaMap[fileMetaData.GetFilename()]; ok {
		if fileMetaData.Version-m.FileMetaMap[fileMetaData.GetFilename()].Version != 1 {
			version = -1
		} else {
			m.FileMetaMap[fileMetaData.GetFilename()].Version += 1
			m.FileMetaMap[fileMetaData.GetFilename()].BlockHashList = fileMetaData.GetBlockHashList()
			version = m.FileMetaMap[fileMetaData.GetFilename()].Version
		}
	} else {
		version = 1
		m.FileMetaMap[fileMetaData.GetFilename()] = &FileMetaData{
			Filename:      fileMetaData.GetFilename(),
			Version:       1,
			BlockHashList: fileMetaData.GetBlockHashList(),
		}
	}
	PrintMetaMap(m.FileMetaMap)
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	if m.BlockStoreAddr != "" {
		return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
	}

	return nil, errors.New("Empty BlockStore Address")
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
