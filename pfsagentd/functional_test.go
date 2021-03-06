package main

import (
	"bytes"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/fission"
)

var (
	testFile         *os.File
	testFileContents []byte
	testFilePath     string
	testFileLastByte byte
)

type testLocksChildStruct struct {
	fileInode    *fileInodeStruct
	exclusive    bool
	lockedChan   chan struct{}  // child writes to chan to indicate it has obtained the lock
	unlockedChan chan struct{}  // child writes to chan to indicate it has released the lock
	getDone      sync.WaitGroup // child calls .Done() on this once it's get{Shared|Exclusive}Lock() returns
	releaseDo    sync.WaitGroup // parent calls .Done() on this when it wants child to call release()
	releaseDone  sync.WaitGroup // child calls .Done() on this once it's release() returns
}

func (fileInode *fileInodeStruct) testLocksChildStructCreate(exclusive bool) (testLocksChild *testLocksChildStruct) {
	testLocksChild = &testLocksChildStruct{
		fileInode:    fileInode,
		exclusive:    exclusive,
		lockedChan:   make(chan struct{}, 1),
		unlockedChan: make(chan struct{}, 1),
	}

	testLocksChild.getDone.Add(1)
	testLocksChild.releaseDo.Add(1)
	testLocksChild.releaseDone.Add(1)

	return
}

func (testLocksChild *testLocksChildStruct) lockObtained() (lockObtained bool) {
	select {
	case _ = <-testLocksChild.lockedChan:
		lockObtained = true
	default:
		lockObtained = false
	}
	return
}

func (testLocksChild *testLocksChildStruct) lockReleased() (lockReleased bool) {
	select {
	case _ = <-testLocksChild.unlockedChan:
		lockReleased = true
	default:
		lockReleased = false
	}
	return
}

func (testLocksChild *testLocksChildStruct) launch() {
	var (
		grantedLock *fileInodeLockRequestStruct
	)

	if testLocksChild.exclusive {
		grantedLock = testLocksChild.fileInode.getExclusiveLock()
	} else {
		grantedLock = testLocksChild.fileInode.getSharedLock()
	}

	testLocksChild.getDone.Done()

	testLocksChild.lockedChan <- struct{}{}

	testLocksChild.releaseDo.Wait()

	grantedLock.release()

	testLocksChild.unlockedChan <- struct{}{}

	testLocksChild.releaseDone.Done()
}

func TestLocks(t *testing.T) {
	const (
		testLocksChildLockedCheckDelay = 250 * time.Millisecond
		testFileName                   = "testLocksFileName"
		testLookupUnique               = uint64(0x12345678)
	)
	var (
		err                      error
		errno                    syscall.Errno
		inHeader                 *fission.InHeader
		lookupIn                 *fission.LookupIn
		lookupOut                *fission.LookupOut
		testFileInode            *fileInodeStruct
		testLocksChildExclusive3 *testLocksChildStruct
		testLocksChildExclusive4 *testLocksChildStruct
		testLocksChildExclusive6 *testLocksChildStruct
		testLocksChildShared1    *testLocksChildStruct
		testLocksChildShared2    *testLocksChildStruct
		testLocksChildShared5    *testLocksChildStruct
		testLocksChildShared7    *testLocksChildStruct
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}
	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}

	inHeader = &fission.InHeader{
		Len:     uint32(fission.InHeaderSize + len(testFileName)),
		OpCode:  fission.OpCodeLookup,
		Unique:  testLookupUnique,
		NodeID:  1,
		UID:     0,
		GID:     0,
		PID:     0,
		Padding: 0,
	}

	lookupIn = &fission.LookupIn{
		Name: []byte(testFileName),
	}

	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if 0 != errno {
		t.Fatalf("DoLookup() failed: %v", errno)
	}

	testFileInode = referenceFileInode(inode.InodeNumber(lookupOut.NodeID))
	if nil == testFileInode {
		t.Fatalf("referenceFileInode(lookupOut.NodeID) should have succeeded")
	}

	testLocksChildExclusive3 = testFileInode.testLocksChildStructCreate(true)
	testLocksChildExclusive4 = testFileInode.testLocksChildStructCreate(true)
	testLocksChildExclusive6 = testFileInode.testLocksChildStructCreate(true)

	testLocksChildShared1 = testFileInode.testLocksChildStructCreate(false)
	testLocksChildShared2 = testFileInode.testLocksChildStructCreate(false)
	testLocksChildShared5 = testFileInode.testLocksChildStructCreate(false)
	testLocksChildShared7 = testFileInode.testLocksChildStructCreate(false)

	go testLocksChildShared1.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared1.lockObtained() {
		t.Fatalf("testLocksChildShared1.lockObtained() should have been true")
	}
	testLocksChildShared1.getDone.Wait()

	go testLocksChildShared2.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared2.lockObtained() {
		t.Fatalf("testLocksChildShared2.lockObtained() should have been true")
	}
	testLocksChildShared2.getDone.Wait()

	go testLocksChildExclusive3.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if testLocksChildExclusive3.lockObtained() {
		t.Fatalf("testLocksChildExclusive3.lockObtained() should have been false")
	}

	go testLocksChildExclusive4.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if testLocksChildExclusive4.lockObtained() {
		t.Fatalf("testLocksChildExclusive4.lockObtained() should have been false")
	}

	go testLocksChildShared5.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if testLocksChildShared5.lockObtained() {
		t.Fatalf("testLocksChildShared5.lockObtained() should have been false")
	}

	go testLocksChildExclusive6.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if testLocksChildExclusive6.lockObtained() {
		t.Fatalf("testLocksChildExclusive6.lockObtained() should have been false")
	}

	go testLocksChildShared7.launch()
	time.Sleep(testLocksChildLockedCheckDelay)
	if testLocksChildShared7.lockObtained() {
		t.Fatalf("testLocksChildShared7.lockObtained() should have been false")
	}

	testLocksChildShared1.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared1.lockReleased() {
		t.Fatalf("testLocksChildShared1.lockReleased() should have been true")
	}
	testLocksChildShared1.releaseDone.Wait()

	if testLocksChildExclusive3.lockObtained() {
		t.Fatalf("testLocksChildExclusive3.lockObtained() should have been false")
	}

	testLocksChildShared2.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared2.lockReleased() {
		t.Fatalf("testLocksChildShared2.lockReleased() should have been true")
	}
	testLocksChildShared2.releaseDone.Wait()

	if !testLocksChildExclusive3.lockObtained() {
		t.Fatalf("testLocksChildExclusive3.lockObtained() should have been true")
	}
	if testLocksChildExclusive4.lockObtained() {
		t.Fatalf("testLocksChildExclusive4.lockObtained() should have been false")
	}

	testLocksChildExclusive3.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildExclusive3.lockReleased() {
		t.Fatalf("testLocksChildExclusive3.lockReleased() should have been true")
	}
	testLocksChildExclusive3.releaseDone.Wait()

	if !testLocksChildExclusive4.lockObtained() {
		t.Fatalf("testLocksChildExclusive4.lockObtained() should have been true")
	}
	if testLocksChildShared5.lockObtained() {
		t.Fatalf("testLocksChildShared5.lockObtained() should have been false")
	}

	testLocksChildExclusive4.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildExclusive4.lockReleased() {
		t.Fatalf("testLocksChildExclusive4.lockReleased() should have been true")
	}
	testLocksChildExclusive4.releaseDone.Wait()

	if !testLocksChildShared5.lockObtained() {
		t.Fatalf("testLocksChildShared5.lockObtained() should have been true")
	}
	if testLocksChildExclusive6.lockObtained() {
		t.Fatalf("testLocksChildExclusive6.lockObtained() should have been false")
	}

	testLocksChildShared5.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared5.lockReleased() {
		t.Fatalf("testLocksChildShared5.lockReleased() should have been true")
	}
	testLocksChildShared5.releaseDone.Wait()

	if !testLocksChildExclusive6.lockObtained() {
		t.Fatalf("testLocksChildExclusive6.lockObtained() should have been true")
	}
	if testLocksChildShared7.lockObtained() {
		t.Fatalf("testLocksChildShared7.lockObtained() should have been false")
	}

	testLocksChildExclusive6.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildExclusive6.lockReleased() {
		t.Fatalf("testLocksChildExclusive6.lockReleased() should have been true")
	}
	testLocksChildExclusive6.releaseDone.Wait()

	if !testLocksChildShared7.lockObtained() {
		t.Fatalf("testLocksChildShared7.lockObtained() should have been true")
	}

	testLocksChildShared7.releaseDo.Done()
	time.Sleep(testLocksChildLockedCheckDelay)
	if !testLocksChildShared7.lockReleased() {
		t.Fatalf("testLocksChildShared7.lockReleased() should have been true")
	}
	testLocksChildShared7.releaseDone.Wait()

	testFileInode.dereference()

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}

func TestSimpleWriteReadClose(t *testing.T) {
	const (
		testFileName       = "testSimpleWriteReadCloseFileName"
		totalBytes   int64 = 100
		randSeed     int64 = 0x0123456789ABCDEF
	)
	var (
		err         error
		readBackBuf []byte
		writtenBuf  []byte
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}

	writtenBuf = make([]byte, totalBytes)

	rand.Seed(randSeed)

	_, _ = rand.Read(writtenBuf)

	_, err = testFile.WriteAt(writtenBuf, 0)
	if nil != err {
		t.Fatalf("testFile.WriteAt(writtenBuf, 0) failed: %v", err)
	}

	readBackBuf = make([]byte, totalBytes)

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's")
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}

func TestRandomOverwrites(t *testing.T) {
	const (
		testFileName       = "testRandomeOverwritesFileName"
		totalBytes   int64 = 1000
		minWriteSize int64 = 1
		maxWriteSize int64 = 10
		numWrites    int64 = 10000
		randSeed     int64 = 0x0123456789ABCDEF
	)
	var (
		err           error
		readBackBuf   []byte
		toWriteBuf    []byte
		toWriteBufLen int64
		toWriteOffset int64
		writeNumber   int64
		writtenBuf    []byte
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}

	writtenBuf = make([]byte, totalBytes)

	rand.Seed(randSeed)

	_, _ = rand.Read(writtenBuf)

	_, err = testFile.WriteAt(writtenBuf, 0)
	if nil != err {
		t.Fatalf("testFile.WriteAt(writtenBuf, 0) failed: %v", err)
	}

	toWriteBuf = make([]byte, maxWriteSize)

	for writeNumber = 0; writeNumber < numWrites; writeNumber++ {
		toWriteBufLen = minWriteSize + rand.Int63n(maxWriteSize-minWriteSize+1)
		toWriteOffset = rand.Int63n(totalBytes - toWriteBufLen + 1)
		_, _ = rand.Read(toWriteBuf[:toWriteBufLen])

		_, err = testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset)
		if nil != err {
			t.Fatalf("testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset) after Create() failed: %v", err)
		}

		_ = copy(writtenBuf[toWriteOffset:(toWriteOffset+toWriteBufLen)], toWriteBuf[:toWriteBufLen])
	}

	readBackBuf = make([]byte, totalBytes)

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) before Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's before Sync()")
	}

	err = testFile.Sync()
	if nil != err {
		t.Fatalf("testFile.Sync() after write pass 1 failed: %v", err)
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after Sync()")
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() after Sync() failed: %v", err)
	}

	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("os.OpenFile(\"%s\",,) failed: %v", testFilePath, err)
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Open() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after Sync()")
	}

	for writeNumber = 0; writeNumber < numWrites; writeNumber++ {
		toWriteBufLen = minWriteSize + rand.Int63n(maxWriteSize-minWriteSize+1)
		toWriteOffset = rand.Int63n(totalBytes - toWriteBufLen + 1)
		_, _ = rand.Read(toWriteBuf[:toWriteBufLen])

		_, err = testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset)
		if nil != err {
			t.Fatalf("testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset) after Open() failed: %v", err)
		}

		_ = copy(writtenBuf[toWriteOffset:(toWriteOffset+toWriteBufLen)], toWriteBuf[:toWriteBufLen])
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after overwrite failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after overwrite")
	}

	err = testFile.Sync()
	if nil != err {
		t.Fatalf("testFile.Sync() after write pass 2 failed: %v", err)
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() after Open() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}

func optionallyReopenTestFile(t *testing.T, prefix string, reopenBeforeEachWrite bool, step string) {
	var (
		err error
	)

	err = testFile.Close()
	if nil != err {
		t.Fatalf("%s: testFile.Close() before \"%s\" failed: %v", prefix, step, err)
	}

	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("%s: os.OpenFile(\"%s\",,) before \"%s\" failed: %v", prefix, testFilePath, step, err)
	}
}

func writeNextByteSlice(t *testing.T, prefix string, offset int, toWriteLen int, step string) {
	var (
		err        error
		extension  int
		index      int
		toWriteBuf []byte
	)

	extension = (offset + toWriteLen) - len(testFileContents)

	if 0 < extension {
		testFileContents = append(testFileContents, make([]byte, extension)...)
	}

	toWriteBuf = make([]byte, toWriteLen)

	for index = 0; index < toWriteLen; index++ {
		if math.MaxUint8 == testFileLastByte {
			testFileLastByte = 1
		} else {
			testFileLastByte++
		}
		toWriteBuf[index] = testFileLastByte
		testFileContents[offset+index] = testFileLastByte
	}

	_, err = testFile.WriteAt(toWriteBuf, int64(offset))
	if nil != err {
		t.Fatalf("%s: testFile.WriteAt(,0) for \"%s\" failed: %v", prefix, step, err)
	}
}

func optionallyFlushTestFile(t *testing.T, prefix string, flushAfterEachWrite bool, step string) {
	var (
		err error
	)

	if flushAfterEachWrite {
		err = testFile.Sync()
		if nil != err {
			t.Fatalf("%s: testFile.Sync() for \"%s\" failed: %v", prefix, step, err)
		}
	}
}

func verifyTestFileContents(t *testing.T, prefix string, step string) {
	var (
		err         error
		readBackBuf []byte
	)

	_, err = testFile.Seek(0, os.SEEK_SET)
	if nil != err {
		t.Fatalf("%s: testFile.Seek(0, os.SEEK_SET) for \"%s\" failed: %v", prefix, step, err)
	}
	readBackBuf, err = ioutil.ReadAll(testFile)
	if nil != err {
		t.Fatalf("%s: ioutil.ReadAll(testFile) for \"%s\" failed: %v", prefix, step, err)
	}
	if 0 != bytes.Compare(readBackBuf, testFileContents) {
		t.Fatalf("%s: bytes.Compare(readBackBuf, testFileContents) for \"%s\" reports unequal bufs", prefix, step)
	}
}

func testExhaustiveOverwrites(t *testing.T, prefix string, reopenBeforeEachWrite bool, flushAfterEachWrite bool) {
	// Non-overlapping extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Non-overlapping extent")
	writeNextByteSlice(t, prefix, 0, 3, "Non-overlapping extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Non-overlapping extent")
	verifyTestFileContents(t, prefix, "Non-overlapping extent")

	// Overlapping existing extent precisely

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping existing extent precisely")
	writeNextByteSlice(t, prefix, 0, 3, "Overlapping existing extent precisely")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping existing extent precisely")
	verifyTestFileContents(t, prefix, "Overlapping existing extent precisely")

	// Overlapping the right of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the right of an existing extent")
	writeNextByteSlice(t, prefix, 2, 3, "Overlapping the right of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the right of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the right of an existing extent")

	// Overlapping the middle of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the middle of an existing extent")
	writeNextByteSlice(t, prefix, 1, 3, "Overlapping the middle of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the middle of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the middle of an existing extent")

	// Overlapping the left of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the left of an existing extent")
	writeNextByteSlice(t, prefix, 8, 3, "Overlapping the left of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the left of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the left of an existing extent")
	writeNextByteSlice(t, prefix, 6, 3, "Overlapping the left of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the left of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the left of an existing extent")

	// Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	writeNextByteSlice(t, prefix, 12, 3, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	verifyTestFileContents(t, prefix, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	writeNextByteSlice(t, prefix, 1, 13, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	verifyTestFileContents(t, prefix, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
}

func TestExhaustiveOverwrites(t *testing.T) {
	const (
		testFileName = "testExhaustiveOverwritesFileName"
	)
	var (
		err error
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}
	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}
	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("os.OpenFile(\"%s\",,) failed: %v", testFilePath, err)
	}

	testFileContents = []byte{}

	testExhaustiveOverwrites(t, "Phase1", false, false)
	testExhaustiveOverwrites(t, "Phase2", false, true)
	testExhaustiveOverwrites(t, "Phase2", true, false)
	testExhaustiveOverwrites(t, "Phase2", true, true)

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}
