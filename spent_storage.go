package main

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/iotaledger/hive.go/kvstore"
	"github.com/iotaledger/hive.go/objectstorage"

	"github.com/gohornet/hornet/pkg/model/hornet"
)

var (
	spentAddressesLock        sync.RWMutex
	errOperationAborted            = errors.New("operation was aborted")
	errTransactionNotFound         = errors.New("transaction not found")
	storePrefixSpentAddresses byte = 15
)

func readLockSpentAddresses() {
	spentAddressesLock.RLock()
}

func readUnlockSpentAddresses() {
	spentAddressesLock.RUnlock()
}

func writeLockSpentAddresses() {
	spentAddressesLock.Lock()
}

func writeUnlockSpentAddresses() {
	spentAddressesLock.Unlock()
}

type cachedSpentAddress struct {
	objectstorage.CachedObject
}

func (c *cachedSpentAddress) getSpentAddress() *hornet.SpentAddress {
	return c.Get().(*hornet.SpentAddress)
}

func spentAddressFactory(key []byte) (objectstorage.StorableObject, int, error) {
	sa := hornet.NewSpentAddress(key[:49])
	return sa, 49, nil
}

func getSpentAddressesStorageSize(spentAddressesStorage *objectstorage.ObjectStorage) int {
	return spentAddressesStorage.GetSize()
}

func configureSpentAddressesStorage(store kvstore.KVStore) *objectstorage.ObjectStorage {

	//opts := profile.LoadProfile().Caches.SpentAddresses

	return objectstorage.New(
		store.WithRealm([]byte{storePrefixSpentAddresses}),
		spentAddressFactory,
		objectstorage.CacheTime(time.Duration(0)*time.Millisecond),
		objectstorage.PartitionKey(49),
		objectstorage.KeysOnly(true),
		objectstorage.LeakDetectionEnabled(false))
	/*
		store.WithRealm([]byte{storePrefixSpentAddresses}),
		spentAddressFactory,
		objectstorage.PartitionKey(0, 49),
		objectstorage.LeakDetectionEnabled(true))
	*/
}

// spentAddress +-0
func wasAddressSpentFrom(address hornet.Hash, spentAddressesStorage *objectstorage.ObjectStorage) bool {
	return spentAddressesStorage.Contains(address)
}

// spentAddress +-0
func markAddressAsSpent(address hornet.Hash, spentAddressesStorage *objectstorage.ObjectStorage) bool {
	spentAddressesLock.Lock()
	defer spentAddressesLock.Unlock()

	return markAddressAsSpentWithoutLocking(address, spentAddressesStorage)
}

// spentAddress +-0
func markAddressAsSpentWithoutLocking(address hornet.Hash, spentAddressesStorage *objectstorage.ObjectStorage) bool {

	spentAddress, _, _ := spentAddressFactory(address)

	newlyAdded := false
	spentAddressesStorage.ComputeIfAbsent(spentAddress.ObjectStorageKey(), func(key []byte) objectstorage.StorableObject {
		newlyAdded = true
		spentAddress.Persist()
		spentAddress.SetModified()
		return spentAddress
	}).Release(true)

	return newlyAdded
}

// StreamSpentAddressesToWriter streams all spent addresses directly to an io.Writer.
func streamSpentAddressesToWriter(buf io.Writer, abortSignal <-chan struct{}, spentAddressesStorage *objectstorage.ObjectStorage) (int32, error) {

	var addressesWritten int32

	wasAborted := false
	spentAddressesStorage.ForEachKeyOnly(func(key []byte) bool {
		select {
		case <-abortSignal:
			wasAborted = true
			return false
		default:
		}

		addressesWritten++
		return binary.Write(buf, binary.LittleEndian, key) == nil
	}, false)

	if wasAborted {
		return 0, errOperationAborted
	}

	return addressesWritten, nil
}

func shutdownSpentAddressesStorage(spentAddressesStorage *objectstorage.ObjectStorage) {
	spentAddressesStorage.Shutdown()
}
