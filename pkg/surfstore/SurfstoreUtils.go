package surfstore

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"reflect"
)

func validateBlockList(value []string, val []string) bool {

	return !reflect.DeepEqual(val, value)
	// flag := false
	// if len(val) == len(value) {
	// 	for i, _ := range val {
	// 		fmt.Println(i)
	// 		if !reflect.DeepEqual(val[i], value[i]) {
	// 			flag = true
	// 			break
	// 		}
	// 	}
	// } else {
	// 	flag = true
	// }

	// return flag
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// TODO - Handle Base Directory related issues
	files, e := ioutil.ReadDir(client.BaseDir)

	// Map to store the final index.txt
	var originalFileMetaMap map[string]*FileMetaData

	// Map to store the new or updated local files
	changedFileMetaMap := make(map[string]*FileMetaData)
	if e != nil {
		panic("Error while fetching file list from base directory")
	}
	originalFileMetaMap = createFileMetaMap(client.BaseDir, files, client.BlockSize)
	for k, v := range originalFileMetaMap {
		changedFileMetaMap[k] = v
	}

	fileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		panic("Error while reading Meta File")
	}
	// Check if index.txt exists or not
	if len(fileMetaMap) != 0 {
		for key, value := range fileMetaMap {
			if val, ok := originalFileMetaMap[key]; ok {
				//Test this

				flag := validateBlockList(value.BlockHashList, val.BlockHashList)
				// fmt.Println("Flag :", flag)
				if !flag {
					fmt.Println("No Local Change for file ", val.GetFilename())
					originalFileMetaMap[key].Version = value.GetVersion()
					delete(changedFileMetaMap, value.GetFilename())
				} else {
					val.Version = value.Version + 1
					changedFileMetaMap[key] = val
					originalFileMetaMap[key] = val
				}
			} else {
				// Delete Case
				if reflect.DeepEqual(value.BlockHashList, []string{"0"}) {
					// Already Deleted File
					fmt.Println("Already Deleted Case Invoked")
					originalFileMetaMap[key] = value
					continue
				} else {
					value.Version += 1
					value.BlockHashList = []string{"0"}
					originalFileMetaMap[key] = value
					changedFileMetaMap[key] = value
				}
			}
		}
	} else {
		fmt.Println("Index.txt is empty")
	}

	fmt.Println("After verifying unchanged files")
	PrintMetaMap(changedFileMetaMap)

	// Get server meta map
	var serverFileInfoMap *map[string]*FileMetaData
	serverFileInfoMap = new(map[string]*FileMetaData)
	err = client.GetFileInfoMap(serverFileInfoMap)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Server File Map")
	PrintMetaMap(*serverFileInfoMap)
	// Check if server has an empty meta file map
	if *serverFileInfoMap != nil {
		for _, value := range *serverFileInfoMap {
			if val, ok := originalFileMetaMap[value.GetFilename()]; ok {
				if val.Version == value.Version {
					flag := validateBlockList(val.BlockHashList, value.BlockHashList)
					if !flag {
						// Case when file is not changed either on server or on local
						continue
					} else {
						// Changed on local as well as remote, in that case, sync from remote
						updateFileFromServer(client, value)
						originalFileMetaMap[value.GetFilename()] = value
						delete(changedFileMetaMap, value.GetFilename())
					}
				} else if val.Version-value.Version != 1 {
					// Case when file is updated locally

					// Case Delete
					if reflect.DeepEqual(value.BlockHashList, []string{"0"}) {
						fmt.Println("Delete Case Invoked for file ", val.GetFilename())
						os.Remove(client.BaseDir + "/" + val.GetFilename())
					} else {
						// Case Update
						fmt.Println("Update Case Invoked for file ", val.GetFilename())
						updateFileFromServer(client, value)
					}
					originalFileMetaMap[value.GetFilename()] = value
				}
			} else {
				//Update originalMetaMap to create the new index.txt
				updateFileFromServer(client, value)
				originalFileMetaMap[value.GetFilename()] = value
			}
		}
	}

	// fmt.Println("After verifying files from server")
	// PrintMetaMap(changedFileMetaMap)

	// Write files which are new or ahead in local
	for _, value := range changedFileMetaMap {
		err := pushFileToServer(client, value)
		if err != nil {
			// Case when another client updated the same file first
			if err.Error() == "sync" {
				// Get the updated serverInfoMap for updated block hashes
				fmt.Println("Sync Error Invoked")
				serverFileInfoMap = new(map[string]*FileMetaData)
				err = client.GetFileInfoMap(serverFileInfoMap)
				if err != nil {
					log.Fatalln(err)
				}
				for _, val := range *serverFileInfoMap {
					if value.GetFilename() == val.GetFilename() {
						updateFileFromServer(client, val)
						originalFileMetaMap[value.GetFilename()] = val
						break
					}
				}
			} else {
				panic(err)
			}
		}
	}

	// After all the files are written to server, update the local index.txt
	err = WriteMetaFile(originalFileMetaMap, client.BaseDir)
	if err != nil {
		panic("Error while writing index.txt")
	}

}

func updateFileFromServer(client RPCClient, fileMetaData *FileMetaData) error {
	var blockStoreAddr *string = new(string)
	client.GetBlockStoreAddr(blockStoreAddr)

	// Delete Case
	if reflect.DeepEqual(fileMetaData.BlockHashList, []string{"0"}) {
		os.Remove(client.BaseDir + "/" + fileMetaData.GetFilename())
	} else {
		fmt.Println("Create New file on local")
		newF, err := os.Create(client.BaseDir + "/" + fileMetaData.GetFilename())
		if err != nil {
			fmt.Println("Error while creating new file")
			return errors.New("error while creating new file")
		}
		for _, hash := range fileMetaData.BlockHashList {
			var block *Block = new(Block)
			// block = new(Block)

			err = client.GetBlock(hash, *blockStoreAddr, block)
			if err != nil {
				return err
			}
			_, err = newF.WriteString(string(block.GetBlockData()))

			if err != nil {
				return err
			}
		}
	}
	return nil
}

func pushFileToServer(client RPCClient, fileMetaData *FileMetaData) error {

	var latestVersion *int32 = new(int32)
	err := client.UpdateFile(fileMetaData, latestVersion)
	if err != nil {
		panic(err)
	}
	if *latestVersion != -1 {

		if reflect.DeepEqual(fileMetaData.BlockHashList, []string{"0"}) {
			return nil
		} else {
			data, err := os.ReadFile(client.BaseDir + "/" + fileMetaData.GetFilename())
			if err != nil {
				fmt.Println("Error while reading file")
				panic(err)
			}

			var blockStoreAddr *string = new(string)
			client.GetBlockStoreAddr(blockStoreAddr)

			for i := 0; i < len(data); i += client.BlockSize {
				var success *bool = new(bool)
				if len(data)-i < client.BlockSize {
					client.PutBlock(&Block{BlockData: data[i:], BlockSize: int32(len(data[i:]))}, *blockStoreAddr, success)
				} else {
					client.PutBlock(&Block{BlockData: data[i : i+client.BlockSize], BlockSize: int32(client.BlockSize)}, *blockStoreAddr, success)
				}

				if !*success {
					return errors.New("Error while storing data to server")
				}
			}
		}
	} else {
		// Handle sync
		return errors.New("sync")
	}

	return nil

}

func createFileMetaMap(baseDir string, files []fs.FileInfo, BlockSize int) map[string]*FileMetaData {

	originalFileMetaMap := make(map[string]*FileMetaData)
	for _, file := range files {
		if file.IsDir() {
			panic("Another Directory Found in the Base Directory")
		}
		var err error
		if file.Name() == "index.txt" {
			continue
		}
		originalFileMetaMap[file.Name()], err = createFileMetaDataFromFile(baseDir, file.Name(), BlockSize)

		if err != nil {
			panic("Error while creating File Meta Map")
		}
	}

	return originalFileMetaMap
}

func createFileMetaDataFromFile(baseDir string, fileName string, BlockSize int) (*FileMetaData, error) {
	data, err := os.ReadFile(baseDir + "/" + fileName)
	if err != nil {
		panic("Error while reading local file")
	}
	hashes := make([]string, 0)
	for i := 0; i < len(data); i += BlockSize {
		if len(data)-i < BlockSize {
			hashValue := GetBlockHashString(data[i:])
			hashes = append(hashes, hashValue)
		} else {
			hashValue := GetBlockHashString(data[i : i+BlockSize])
			hashes = append(hashes, hashValue)
		}

	}

	return &FileMetaData{
		Filename:      fileName,
		Version:       int32(1),
		BlockHashList: hashes,
	}, nil
}
