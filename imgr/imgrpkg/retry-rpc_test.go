// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/sortedmap"
)

const (
	testFileInodeExtentMapCacheEvictLowLimit  uint64 = 100
	testFileInodeExtentMapCacheEvictHighLimit uint64 = 110

	testFileInodeExtentMapMaxKeysPerNode = 100
)

type testInodeHeadLayoutEntryV1Struct struct {
	objectSize      uint64
	bytesReferenced uint64
}

type testFileInodeStruct struct {
	inodeHeadV1                              *ilayout.InodeHeadV1Struct
	layoutAsMap                              map[uint64]*testInodeHeadLayoutEntryV1Struct // key == ilayout.InodeHeadLayoutEntryV1Struct; value == remaining fields of ilayout.InodeHeadLayoutEntryV1Struct
	superBlockInodeObjectCountAdjustment     int64                                        // to be sent in next PutInodeTableEntriesRequest
	superBlockInodeObjectSizeAdjustment      int64                                        // to be sent in next PutInodeTableEntriesRequest
	superBlockInodeBytesReferencedAdjustment int64                                        // to be sent in next PutInodeTableEntriesRequest
	dereferencedObjectNumberArray            []uint64                                     // to be sent in next PutInodeTableEntriesRequest
	extentMapCache                           sortedmap.BPlusTreeCache
	extentMap                                sortedmap.BPlusTree
	putObjectNumber                          uint64
	putObjectBuffer                          *bytes.Buffer //                                if nil, testFileInodeStruct not open
}

func newTestFileInode(inodeNumber uint64) (testFileInode *testFileInodeStruct) {
	var (
		timeNow = time.Now()
	)

	testFileInode = &testFileInodeStruct{
		inodeHeadV1: &ilayout.InodeHeadV1Struct{
			InodeNumber:         inodeNumber,
			InodeType:           ilayout.InodeTypeFile,
			LinkTable:           []ilayout.InodeLinkTableEntryStruct{},
			Size:                0,
			ModificationTime:    timeNow,
			StatusChangeTime:    timeNow,
			Mode:                ilayout.InodeModeMask,
			UserID:              0,
			GroupID:             0,
			StreamTable:         []ilayout.InodeStreamTableEntryStruct{},
			PayloadObjectNumber: ilayout.CheckPointObjectNumber,
			PayloadObjectOffset: 0,
			PayloadObjectLength: 0,
			SymLinkTarget:       "",
			Layout:              []ilayout.InodeHeadLayoutEntryV1Struct{},
		},
		layoutAsMap:                              make(map[uint64]*testInodeHeadLayoutEntryV1Struct),
		superBlockInodeObjectCountAdjustment:     0,
		superBlockInodeObjectSizeAdjustment:      0,
		superBlockInodeBytesReferencedAdjustment: 0,
		dereferencedObjectNumberArray:            []uint64{},
		extentMapCache:                           sortedmap.NewBPlusTreeCache(testFileInodeExtentMapCacheEvictLowLimit, testFileInodeExtentMapCacheEvictHighLimit),
		extentMap:                                nil, // filled in just below
		putObjectNumber:                          ilayout.CheckPointObjectNumber,
		putObjectBuffer:                          nil,
	}

	testFileInode.extentMap = sortedmap.NewBPlusTree(testFileInodeExtentMapMaxKeysPerNode, sortedmap.CompareUint64, testFileInode, testFileInode.extentMapCache)

	return
}

func oldTestFileInode(inodeHeadV1 *ilayout.InodeHeadV1Struct) (testFileInode *testFileInodeStruct, err error) {
	var (
		inodeHeadLayoutEntryV1 ilayout.InodeHeadLayoutEntryV1Struct
	)

	if inodeHeadV1.InodeType != ilayout.InodeTypeFile {
		err = fmt.Errorf("oldTestFileInode() called with inodeHeadV1.InodeType == %v... must be ilayout.InodeTypeFile (%v)", inodeHeadV1.InodeType, ilayout.InodeTypeFile)
		return
	}

	testFileInode = &testFileInodeStruct{
		inodeHeadV1:                              inodeHeadV1,
		layoutAsMap:                              make(map[uint64]*testInodeHeadLayoutEntryV1Struct),
		superBlockInodeObjectCountAdjustment:     0,
		superBlockInodeObjectSizeAdjustment:      0,
		superBlockInodeBytesReferencedAdjustment: 0,
		dereferencedObjectNumberArray:            []uint64{},
		extentMapCache:                           sortedmap.NewBPlusTreeCache(testFileInodeExtentMapCacheEvictLowLimit, testFileInodeExtentMapCacheEvictHighLimit),
		extentMap:                                nil, // filled in just below
		putObjectNumber:                          ilayout.CheckPointObjectNumber,
		putObjectBuffer:                          nil,
	}

	for _, inodeHeadLayoutEntryV1 = range testFileInode.inodeHeadV1.Layout {
		testFileInode.layoutAsMap[inodeHeadLayoutEntryV1.ObjectNumber] = &testInodeHeadLayoutEntryV1Struct{
			objectSize:      inodeHeadLayoutEntryV1.ObjectSize,
			bytesReferenced: inodeHeadLayoutEntryV1.BytesReferenced,
		}
	}

	testFileInode.extentMap, err = sortedmap.OldBPlusTree(inodeHeadV1.PayloadObjectNumber, inodeHeadV1.PayloadObjectOffset, inodeHeadV1.PayloadObjectLength, sortedmap.CompareUint64, testFileInode, testFileInode.extentMapCache)
	if nil != err {
		err = fmt.Errorf("sortedmap.OldBPlusTree() for InodeHeadV1: %#v failed: %v", inodeHeadV1, err)
		return
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) externalizeInodeHeadV1Layout() {
	var (
		layoutIndex                int
		objectNumber               uint64
		testInodeHeadLayoutEntryV1 *testInodeHeadLayoutEntryV1Struct
	)

	testFileInode.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, len(testFileInode.layoutAsMap))
	layoutIndex = 0

	for objectNumber, testInodeHeadLayoutEntryV1 = range testFileInode.layoutAsMap {
		testFileInode.inodeHeadV1.Layout[layoutIndex] = ilayout.InodeHeadLayoutEntryV1Struct{
			ObjectNumber:    objectNumber,
			ObjectSize:      testInodeHeadLayoutEntryV1.objectSize,
			BytesReferenced: testInodeHeadLayoutEntryV1.bytesReferenced,
		}

		layoutIndex++
	}
}

func (testFileInode *testFileInodeStruct) openObject(objectNumber uint64) (err error) {
	if testFileInode.putObjectBuffer != nil {
		err = fmt.Errorf("openObject() called while putObjectBuffer set")
		return
	}

	testFileInode.superBlockInodeObjectCountAdjustment = 0
	testFileInode.superBlockInodeObjectSizeAdjustment = 0
	testFileInode.superBlockInodeBytesReferencedAdjustment = 0
	testFileInode.dereferencedObjectNumberArray = []uint64{}

	testFileInode.putObjectNumber = objectNumber
	testFileInode.putObjectBuffer = new(bytes.Buffer)

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) closeObject() (err error) {
	var (
		putRequestHeaders http.Header
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("closeObject() called while putObjectBuffer unset")
		return
	}

	putRequestHeaders = make(http.Header)

	putRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.containerURL+"/"+ilayout.GetObjectNameAsString(testFileInode.putObjectNumber), putRequestHeaders, testFileInode.putObjectBuffer)
	if nil != err {
		err = fmt.Errorf("testDoHTTPRequest(\"PUT\", testGlobals.containerURL+\"/\"+ilayout.GetObjectNameAsString(testFileInode.putObjectNumber), putRequestHeaders, &testFileInode.putObjectBuffer) failed: %v", err)
		return
	}

	testFileInode.putObjectBuffer = nil

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) getObjectData(objectNumber uint64, objectOffset uint64, objectLength uint64) (objectData []byte, err error) {
	var (
		getRequestHeaders http.Header
	)

	if objectNumber == ilayout.CheckPointObjectNumber {
		err = fmt.Errorf("getObjectData(objectNumber: %v,,) not supported", objectNumber)
		return
	}

	if objectNumber == testFileInode.putObjectNumber {
		if objectOffset+objectLength > uint64(testFileInode.putObjectBuffer.Len()) {
			err = fmt.Errorf("objectOffset+objectLength [%v] > testFileInode.putObjectBuffer.Len() [%v]", objectOffset+objectLength, testFileInode.putObjectBuffer.Len())
			return
		}

		objectData = testFileInode.putObjectBuffer.Bytes()[objectOffset : objectOffset+objectLength]
	} else {
		getRequestHeaders = make(http.Header)

		getRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}
		getRequestHeaders["Range"] = []string{fmt.Sprintf("bytes=%d-%d", objectOffset, (objectOffset + objectLength - 1))}

		_, objectData, err = testDoHTTPRequest("GET", testGlobals.containerURL+"/"+ilayout.GetObjectNameAsString(objectNumber), getRequestHeaders, nil)
		if nil != err {
			err = fmt.Errorf("testDoHTTPRequest(\"GET\", testGlobals.containerURL+\"/\"+ilayout.GetObjectNameAsString(objectNumber: %v, Range: %v), getRequestHeaders, nil) failed: %v", objectNumber, getRequestHeaders["Range"][0], err)
			return
		}
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) putObjectData(objectData []byte, trackedInLayout bool) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		ok                         bool
		testInodeHeadLayoutEntryV1 *testInodeHeadLayoutEntryV1Struct
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("putObjectData() called while putObjectBuffer unset")
		return
	}

	objectNumber = testFileInode.putObjectNumber
	objectOffset = uint64(testFileInode.putObjectBuffer.Len())

	_, err = testFileInode.putObjectBuffer.Write(objectData)
	if nil != err {
		err = fmt.Errorf("testFileInode.putObjectBuffer.Write(objectData) failed: %v", err)
		return
	}

	if trackedInLayout {
		testInodeHeadLayoutEntryV1, ok = testFileInode.layoutAsMap[testFileInode.putObjectNumber]
		if ok {
			testInodeHeadLayoutEntryV1.objectSize += uint64(len(objectData))
			testInodeHeadLayoutEntryV1.bytesReferenced += uint64(len(objectData))
		} else {
			testInodeHeadLayoutEntryV1 = &testInodeHeadLayoutEntryV1Struct{
				objectSize:      uint64(len(objectData)),
				bytesReferenced: uint64(len(objectData)),
			}

			testFileInode.layoutAsMap[testFileInode.putObjectNumber] = testInodeHeadLayoutEntryV1
		}

		testFileInode.superBlockInodeObjectSizeAdjustment += int64(len(objectData))
		testFileInode.superBlockInodeBytesReferencedAdjustment += int64(len(objectData))
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) discardObjectData(objectNumber uint64, objectLength uint64) (err error) {
	var (
		ok                         bool
		testInodeHeadLayoutEntryV1 *testInodeHeadLayoutEntryV1Struct
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("discardObjectData() called while putObjectBuffer unset")
		return
	}

	testInodeHeadLayoutEntryV1, ok = testFileInode.layoutAsMap[objectNumber]
	if !ok {
		err = fmt.Errorf("testFileInode.layoutAsMap[objectNumber] returned !ok")
		return
	}

	if objectLength > testInodeHeadLayoutEntryV1.bytesReferenced {
		err = fmt.Errorf("objectLength [%v] > testInodeHeadLayoutEntryV1.bytesReferenced [%v]", objectLength, testInodeHeadLayoutEntryV1.bytesReferenced)
		return
	}

	testInodeHeadLayoutEntryV1.bytesReferenced -= objectLength
	testFileInode.superBlockInodeBytesReferencedAdjustment -= int64(objectLength)

	if testInodeHeadLayoutEntryV1.bytesReferenced == 0 {
		delete(testFileInode.layoutAsMap, objectNumber)
		testFileInode.superBlockInodeObjectCountAdjustment--
		testFileInode.superBlockInodeObjectSizeAdjustment -= int64(testInodeHeadLayoutEntryV1.objectSize)
		testFileInode.dereferencedObjectNumberArray = append(testFileInode.dereferencedObjectNumberArray, objectNumber)
	} else {
		testFileInode.layoutAsMap[objectNumber] = testInodeHeadLayoutEntryV1
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		fileOffset uint64
		ok         bool
	)

	fileOffset, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	keyAsString = fmt.Sprintf("%016X", fileOffset)

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		extentMapEntryValueV1     *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1JSON []byte
		ok                        bool
	)

	extentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		return
	}

	extentMapEntryValueV1JSON, err = json.Marshal(extentMapEntryValueV1)
	if nil != err {
		err = fmt.Errorf("json.Marshal(extentMapEntryValueV1) failed: %v", err)
		return
	}

	valueAsString = string(extentMapEntryValueV1JSON[:])

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = testFileInode.getObjectData(objectNumber, objectOffset, objectLength)
	return
}

func (testFileInode *testFileInodeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber, objectOffset, err = testFileInode.putObjectData(nodeByteSlice, true)
	return
}

func (testFileInode *testFileInodeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = testFileInode.discardObjectData(objectNumber, objectLength)
	return
}

func (testFileInode *testFileInodeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		fileOffset uint64
		nextPos    int
		ok         bool
	)

	fileOffset, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset)
	if nil != err {
		err = fmt.Errorf("ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset) failed: %v", err)
		return
	}
	if nextPos != 8 {
		err = fmt.Errorf("ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset) returned unexpected nextPos: %v", nextPos)
		return
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		fileOffset uint64
		nextPos    int
	)

	fileOffset, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if nil != err {
		err = fmt.Errorf("ilayout.GetLEUint64FromBuf(payloadData, 0) failed: %v", err)
		return
	}

	key = fileOffset
	bytesConsumed = uint64(nextPos)
	err = nil
	return
}

func (testFileInode *testFileInodeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		extentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
		ok                    bool
	)

	extentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		return
	}

	packedValue, err = extentMapEntryValueV1.MarshalExtentMapEntryValueV1()

	return
}

func (testFileInode *testFileInodeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt    int
		extentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
	)

	extentMapEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalExtentMapEntryValueV1(payloadData)
	if nil != err {
		err = fmt.Errorf("ilayout.UnmarshalExtentMapEntryValueV1(payloadData) failed: %v", err)
		return
	}

	value = extentMapEntryValueV1
	bytesConsumed = uint64(bytesConsumedAsInt)
	err = nil
	return
}

type testRetryRPCClientCallbacksStruct struct {
	interruptPayloadChan chan []byte
}

func (retryrpcClientCallbacks *testRetryRPCClientCallbacksStruct) Interrupt(payload []byte) {
	retryrpcClientCallbacks.interruptPayloadChan <- payload
}

func TestRetryRPC(t *testing.T) {
	var (
		adjustInodeTableEntryOpenCountRequest  *AdjustInodeTableEntryOpenCountRequestStruct
		adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct
		deleteInodeTableEntryRequest           *DeleteInodeTableEntryRequestStruct
		deleteInodeTableEntryResponse          *DeleteInodeTableEntryResponseStruct
		err                                    error
		fetchNonceRangeRequest                 *FetchNonceRangeRequestStruct
		fetchNonceRangeResponse                *FetchNonceRangeResponseStruct
		fileInodeNumber                        uint64
		fileInodeObjectA                       uint64
		fileInodeObjectB                       uint64
		fileInodeObjectC                       uint64
		flushRequest                           *FlushRequestStruct
		flushResponse                          *FlushResponseStruct
		getInodeTableEntryRequest              *GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse             *GetInodeTableEntryResponseStruct
		inodeHeadV1Buf                         []byte
		leaseRequest                           *LeaseRequestStruct
		leaseResponse                          *LeaseResponseStruct
		mountRequest                           *MountRequestStruct
		mountResponse                          *MountResponseStruct
		ok                                     bool
		postRequestBody                        string
		putInodeTableEntriesRequest            *PutInodeTableEntriesRequestStruct
		putInodeTableEntriesResponse           *PutInodeTableEntriesResponseStruct
		putObjectDataObjectNumber              uint64
		putObjectDataObjectOffset              uint64
		putRequestBody                         string
		renewMountRequest                      *RenewMountRequestStruct
		renewMountResponse                     *RenewMountResponseStruct
		retryrpcClient                         *retryrpc.Client
		retryrpcClientCallbacks                *testRetryRPCClientCallbacksStruct
		testFileInode                          *testFileInodeStruct
		unmountRequest                         *UnmountRequestStruct
		unmountResponse                        *UnmountResponseStruct
	)

	// Setup RetryRPC Client

	retryrpcClientCallbacks = &testRetryRPCClientCallbacksStruct{
		interruptPayloadChan: make(chan []byte),
	}

	// Setup test environment

	testSetup(t, retryrpcClientCallbacks)

	retryrpcClient, err = retryrpc.NewClient(testGlobals.retryrpcClientConfig)
	if nil != err {
		t.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	// Format testVolume

	postRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("POST", testGlobals.httpServerURL+"/volume", nil, strings.NewReader(postRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"POST\", testGlobals.httpServerURL+\"/volume\", nil, strings.NewReader(postRequestBody)) failed: %v", err)
	}

	// Start serving testVolume

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\"}", testGlobals.containerURL)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody)) failed: %v", err)
	}

	// Attempt a FetchNonceRange() without a prior Mount()... which should fail (no Mount)

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: "", // An invalid MountID... so will not be found in globals.mountMap
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) should have failed")
	}

	// Perform a Mount()

	mountRequest = &MountRequestStruct{
		VolumeName: testVolume,
		AuthToken:  testGlobals.authToken,
	}
	mountResponse = &MountResponseStruct{}

	err = retryrpcClient.Send("Mount", mountRequest, mountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Mount(,)\",,) failed: %v", err)
	}

	// Perform a FetchNonceRange()

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: mountResponse.MountID,
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) failed: %v", err)
	}

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (no Lease)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Force a need for a re-auth

	iswiftpkg.ForceReAuth()

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (re-auth required)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Perform a RenewMount()

	err = testDoAuth()
	if nil != err {
		t.Fatalf("testDoAuth() failed: %v", err)
	}

	renewMountRequest = &RenewMountRequestStruct{
		MountID:   mountResponse.MountID,
		AuthToken: testGlobals.authToken,
	}
	renewMountResponse = &RenewMountResponseStruct{}

	err = retryrpcClient.Send("RenewMount", renewMountRequest, renewMountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"RenewMount(,)\",,) failed: %v", err)
	}

	// Fetch a Shared Lease on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypeShared,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypeShared)\",,) failed: %v", err)
	}

	// Perform a GetInodeTableEntry() for RootDirInode

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) failed: %v", err)
	}

	// Attempt a PutInodeTableEntries() for RootDirInode... which should fail (only Shared Lease)

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) should have failed")
	}

	// Perform a Lease Promote on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypePromote,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypePromote)\",,) failed: %v", err)
	}

	// Perform a PutInodeTableEntries() on RootDirInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
		SuperBlockInodeObjectCountAdjustment:     0,
		SuperBlockInodeObjectSizeAdjustment:      0,
		SuperBlockInodeBytesReferencedAdjustment: 0,
		DereferencedObjectNumberArray:            []uint64{},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// Create a FileInode (set LinkCount to 0 & no dir entry)... with small amount of data

	if 2 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeNumber = fetchNonceRangeResponse.NextNonce
	fileInodeObjectA = fileInodeNumber + 1

	fetchNonceRangeResponse.NextNonce += 2
	fetchNonceRangeResponse.NumNoncesFetched -= 2

	testFileInode = newTestFileInode(fileInodeNumber)

	err = testFileInode.openObject(fileInodeObjectA)
	if nil != err {
		t.Fatalf("testFileInode.openObject(fileInodeObjectA) failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData([]byte("ABC"), true)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData([]byte(\"A\"), true) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.putObjectData([]byte(\"A\"), true) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectA (%v)", putObjectDataObjectNumber, fileInodeObjectA)
	}
	if putObjectDataObjectOffset != 0 {
		t.Fatalf("testFileInode.putObjectData([]byte(\"A\"), true) returned unexpected putObjectDataObjectOffset (%v) - expected 0", putObjectDataObjectOffset)
	}

	ok, err = testFileInode.extentMap.Put(uint64(0), &ilayout.ExtentMapEntryValueV1Struct{Length: 3, ObjectNumber: putObjectDataObjectNumber, ObjectOffset: putObjectDataObjectOffset})
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Put(0,) failed: %v", err)
	}
	if !ok {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned !ok")
	}

	testFileInode.inodeHeadV1.PayloadObjectNumber, testFileInode.inodeHeadV1.PayloadObjectOffset, testFileInode.inodeHeadV1.PayloadObjectLength, err = testFileInode.extentMap.Flush(false)
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Flush() failed: %v", err)
	}
	if testFileInode.inodeHeadV1.PayloadObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectNumber: %v", testFileInode.inodeHeadV1.PayloadObjectNumber)
	}
	if testFileInode.inodeHeadV1.PayloadObjectOffset != 3 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectOffset: %v", testFileInode.inodeHeadV1.PayloadObjectOffset)
	}
	if testFileInode.inodeHeadV1.PayloadObjectLength != 58 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectOffset: %v", testFileInode.inodeHeadV1.PayloadObjectOffset)
	}

	testFileInode.externalizeInodeHeadV1Layout()
	if len(testFileInode.inodeHeadV1.Layout) != 1 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), len(testFileInode.inodeHeadV1.Layout) was unexpected: %v", len(testFileInode.inodeHeadV1.Layout))
	}
	if testFileInode.inodeHeadV1.Layout[0].ObjectNumber != fileInodeObjectA {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[0].ObjectSize != 3+58 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].ObjectSize was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].ObjectSize)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesReferenced != 3+58 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesReferenced)
	}

	inodeHeadV1Buf, err = testFileInode.inodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatalf("testFileInode.inodeHeadV1.MarshalInodeHeadV1() failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData(inodeHeadV1Buf, false)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectA (%v)", putObjectDataObjectNumber, fileInodeObjectA)
	}
	if putObjectDataObjectOffset != 3+58 {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectOffset (%v) - expected 0", putObjectDataObjectOffset)
	}

	err = testFileInode.closeObject()
	if nil != err {
		t.Fatalf("testFileInode.closeObject() failed: %v", err)
	}

	// Fetch an Exclusive Lease on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeExclusive,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeExclusive)\",,) failed: %v", err)
	}

	// Perform a PutInodeTableEntries() for FileInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           fileInodeNumber,
				InodeHeadObjectNumber: fileInodeObjectA,
				InodeHeadLength:       uint64(len(inodeHeadV1Buf)),
			},
		},
		SuperBlockInodeObjectCountAdjustment:     testFileInode.superBlockInodeObjectCountAdjustment,
		SuperBlockInodeObjectSizeAdjustment:      testFileInode.superBlockInodeObjectSizeAdjustment,
		SuperBlockInodeBytesReferencedAdjustment: testFileInode.superBlockInodeBytesReferencedAdjustment,
		DereferencedObjectNumberArray:            testFileInode.dereferencedObjectNumberArray,
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// TODO: Remove this early exit skipping of following TODOs

	if nil == err {
		t.Logf("Exiting TestRetryRPC() early to skip following TODOs")
		return
	}

	// TODO: Append some data (to new Object) for FileInode... and new stat

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectB = fileInodeObjectA + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	t.Logf("fileInodeObjectB: %016X", fileInodeObjectB)

	// TODO: Perform a PutInodeTableEntries() for FileInode

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Overwrite the original data in FileInode (to new Object)... and new stat dereferencing 1st Object

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectC = fileInodeObjectB + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	t.Logf("fileInodeObjectC: %016X", fileInodeObjectC)

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that 1st Object for FileInode gets deleted... but not 2nd nor 3rd

	// Perform an AdjustInodeTableEntryOpenCount(+1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  +1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,+1)\",,) failed: %v", err)
	}

	// Perform a DeleteInodeTableEntry() on FileInode

	deleteInodeTableEntryRequest = &DeleteInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
	}
	deleteInodeTableEntryResponse = &DeleteInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("DeleteInodeTableEntry", deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"DeleteInodeTableEntry(,fileInodeNumber)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that FileInode is still in InodeTable

	// Perform an AdjustInodeTableEntryOpenCount(-1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  -1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,-1)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that FileInode is no longer in InodeTable and 2nd and 3rd Objects are deleted

	// Perform a Lease Release on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeRelease,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeRelease)\",,) failed: %v", err)
	}

	// Perform an Unmount()... without first releasing Exclusive Lease on RootDirInode

	unmountRequest = &UnmountRequestStruct{
		MountID: mountResponse.MountID,
	}
	unmountResponse = &UnmountResponseStruct{}

	err = retryrpcClient.Send("Unmount", unmountRequest, unmountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Unmount()\",,) failed: %v", err)
	}

	// TODO: Verify that Exclusive Lease on RootDirInode is implicitly released

	// Teardown RetryRPC Client

	retryrpcClient.Close()

	// And teardown test environment

	testTeardown(t)
}
