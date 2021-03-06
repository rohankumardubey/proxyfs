package headhunter

import (
	"container/list"
	"fmt"
	"hash/crc64"
	"os"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/clientv3"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/trackedlock"
	"github.com/swiftstack/ProxyFS/transitions"
)

const (
	firstNonceToProvide = uint64(2) // Must skip useful values: 0 == unassigned and 1 == RootDirInodeNumber
)

const (
	liveSnapShotID = uint64(0)
)

const (
	AccountHeaderName           = "X-ProxyFS-BiModal"
	AccountHeaderNameTranslated = "X-Account-Sysmeta-Proxyfs-Bimodal"
	AccountHeaderValue          = "true"
	CheckpointHeaderName        = "X-Container-Meta-Checkpoint"
	StoragePolicyHeaderName     = "X-Storage-Policy"
)

const (
	MetadataRecycleBinHeaderName  = "X-Object-Meta-Recycle-Bin"
	MetadataRecycleBinHeaderValue = "true"
)

type bPlusTreeTrackerStruct struct {
	bPlusTreeLayout sortedmap.LayoutReport
}

type bPlusTreeWrapperStruct struct {
	volumeView       *volumeViewStruct
	bPlusTree        sortedmap.BPlusTree
	bPlusTreeTracker *bPlusTreeTrackerStruct // For inodeRecWrapper, logSegmentRecWrapper, & bPlusTreeObjectWrapper:
	//                                            only valid for liveView... nil otherwise
	//                                          For createdObjectsWrapper & deletedObjectsWrapper:
	//                                            all volumeView's share the corresponding one created for liveView
	totalPutNodes uint64 // each call to PutNode() increments this
	totalPutBytes uint64 // each call to PutNode() adds the buffer size
}

type volumeViewStruct struct {
	volume     *volumeStruct
	nonce      uint64 // supplies strict time-ordering of views regardless of timebase resets
	snapShotID uint64 // in the range [1:2^SnapShotIDNumBits-2]
	//                     ID == 0                     reserved for the "live" view
	//                     ID == 2^SnapShotIDNumBits-1 reserved for the .snapshot subdir of a dir
	snapShotTime           time.Time
	snapShotName           string
	inodeRecWrapper        *bPlusTreeWrapperStruct
	logSegmentRecWrapper   *bPlusTreeWrapperStruct
	bPlusTreeObjectWrapper *bPlusTreeWrapperStruct
	createdObjectsWrapper  *bPlusTreeWrapperStruct // if volumeView is     the liveView, should be empty
	//                                                if volumeView is not the liveView, tracks objects created between this and the next volumeView
	deletedObjectsWrapper *bPlusTreeWrapperStruct //  if volumeView is     the liveView, tracks objects to be deleted at next checkpoint
	//                                                if volumeView is not the liveView, tracks objects deleted between this and the next volumeView
	// upon object creation:
	//   if prior volumeView exists:
	//     add object to that volumeView's createdObjects
	//   else:
	//     do nothing
	//
	// upon object deletion:
	//   if prior volumeView exists:
	//     if object in prior volumeView createdObjects:
	//       remove object from prior volumeView createdObjects
	//       add object to liveView deletedObjects
	//     else:
	//       add object to prior volumeView deletedObjects
	//   else:
	//     add object to liveView deletedObjects
	//
	// upon next checkpoint:
	//   remove all objects from liveView deletedObjects
	//   schedule background deletion of those removed objects
	//
	// upon SnapShot creation:
	//   take a fresh checkpoint (will drain liveView deletedObjects)
	//   create a fresh volumeView
	//   take a fresh checkpoint (will record volumeView in this checkpoint)
	//
	// upon volumeView deletion:
	//   if deleted volumeView is oldest:
	//     discard volumeView createdObjects
	//     schedule background deletion of volumeView deletedObjects
	//     discard volumeView deletedObjects
	//   else:
	//     createdObjects merged into prior volumeView
	//     discard volumeView createdObjects
	//     for each object in deletedObjects:
	//       if object in prior volumeView createdObjects:
	//         remove object from prior volumeView createdObjects
	//         schedule background deletion of object
	//       else:
	//         add object to prior volumeView deletedObjects
	//     discard volumeView deletedObjects
	//   discard volumeView
}

type volumeStruct struct {
	trackedlock.Mutex
	volumeName                              string
	accountName                             string
	maxFlushSize                            uint64
	nonceValuesToReserve                    uint16
	maxInodesPerMetadataNode                uint64
	maxLogSegmentsPerMetadataNode           uint64
	maxDirFileNodesPerMetadataNode          uint64
	maxCreatedDeletedObjectsPerMetadataNode uint64
	checkpointEtcdKeyName                   string
	checkpointContainerName                 string
	checkpointContainerStoragePolicy        string
	checkpointInterval                      time.Duration
	replayLogFileName                       string   //           if != "", use replay log to reduce RPO to zero
	replayLogFile                           *os.File //           opened on first Put or Delete after checkpoint
	//                                                            closed/deleted on successful checkpoint
	volumeGroup                             *volumeGroupStruct
	served                                  bool
	defaultReplayLogWriteBuffer             []byte //             used for O_DIRECT writes to replay log
	checkpointTriggeringEvents              uint64 //             count of events modifying metadata since last checkpoint
	checkpointChunkedPutContext             swiftclient.ChunkedPutContext
	checkpointChunkedPutContextObjectNumber uint64 //             ultimately copied to CheckpointObjectTrailerStructObjectNumber
	eventListeners                          map[VolumeEventListener]struct{}
	snapShotIDNumBits                       uint16
	snapShotIDShift                         uint64 //             e.g. inodeNumber >> snapShotIDNumBits == snapShotID
	dotSnapShotDirSnapShotID                uint64 //             .snapshot/ pseudo directory Inode's snapShotID
	snapShotU64NonceMask                    uint64 //             used to mask of snapShotID bits
	maxNonce                                uint64
	nextNonce                               uint64
	checkpointRequestChan                   chan *checkpointRequestStruct
	checkpointHeader                        *CheckpointHeaderStruct
	checkpointHeaderEtcdRevision            int64
	liveView                                *volumeViewStruct
	priorView                               *volumeViewStruct
	postponePriorViewCreatedObjectsPuts     bool
	postponedPriorViewCreatedObjectsPuts    map[uint64]struct{}
	viewTreeByNonce                         sortedmap.LLRBTree // key == volumeViewStruct.Nonce; value == *volumeViewStruct
	viewTreeByID                            sortedmap.LLRBTree // key == volumeViewStruct.ID;    value == *volumeViewStruct
	viewTreeByTime                          sortedmap.LLRBTree // key == volumeViewStruct.Time;  value == *volumeViewStruct
	viewTreeByName                          sortedmap.LLRBTree // key == volumeViewStruct.Name;  value == *volumeViewStruct
	availableSnapShotIDList                 *list.List
	backgroundObjectDeleteWG                sync.WaitGroup
}

type volumeGroupStruct struct {
	name      string
	volumeMap map[string]*volumeStruct // key == volumeStruct.volumeName
}

type globalsStruct struct {
	trackedlock.Mutex

	crc64ECMATable                          *crc64.Table
	uint64Size                              uint64
	ElementOfBPlusTreeLayoutStructSize      uint64
	replayLogTransactionFixedPartStructSize uint64

	inodeRecCache                              sortedmap.BPlusTreeCache
	inodeRecCachePriorCacheHits                uint64
	inodeRecCachePriorCacheMisses              uint64
	logSegmentRecCache                         sortedmap.BPlusTreeCache
	logSegmentRecCachePriorCacheHits           uint64
	logSegmentRecCachePriorCacheMisses         uint64
	bPlusTreeObjectCache                       sortedmap.BPlusTreeCache
	bPlusTreeObjectCachePriorCacheHits         uint64
	bPlusTreeObjectCachePriorCacheMisses       uint64
	createdDeletedObjectsCache                 sortedmap.BPlusTreeCache
	createdDeletedObjectsCachePriorCacheHits   uint64
	createdDeletedObjectsCachePriorCacheMisses uint64

	checkpointHeaderConsensusAttempts uint16
	mountRetryLimit                   uint16
	mountRetryDelay                   []time.Duration

	logCheckpointHeaderPosts bool

	etcdEnabled          bool
	etcdEndpoints        []string
	etcdAutoSyncInterval time.Duration
	etcdDialTimeout      time.Duration
	etcdOpTimeout        time.Duration

	metadataRecycleBin       bool
	metadataRecycleBinHeader map[string][]string

	etcdClient *etcd.Client
	etcdKV     etcd.KV

	volumeGroupMap map[string]*volumeGroupStruct // key == volumeGroupStruct.name
	volumeMap      map[string]*volumeStruct      // key == volumeStruct.volumeName

	backgroundObjectDeleteRWMutex   trackedlock.RWMutex
	backgroundObjectDeleteEnabled   bool           // used to make {Disable|Enable}ObjectDeletions() idempotent
	backgroundObjectDeleteEnabledWG sync.WaitGroup // use to awaken blocked performDelayedObjectDeletes() after EnableObjectDeletions() called
	backgroundObjectDeleteActiveWG  sync.WaitGroup // used to hold off returning from DisableObjectDeletions() until all deletions cease

	FetchNonceUsec                     bucketstats.BucketLog2Round
	GetInodeRecUsec                    bucketstats.BucketLog2Round
	GetInodeRecBytes                   bucketstats.BucketLog2Round
	PutInodeRecUsec                    bucketstats.BucketLog2Round
	PutInodeRecBytes                   bucketstats.BucketLog2Round
	PutInodeRecsUsec                   bucketstats.BucketLog2Round
	PutInodeRecsBytes                  bucketstats.BucketLog2Round
	DeleteInodeRecUsec                 bucketstats.BucketLog2Round
	IndexedInodeNumberUsec             bucketstats.BucketLog2Round
	NextInodeNumberUsec                bucketstats.BucketLog2Round
	GetLogSegmentRecUsec               bucketstats.BucketLog2Round
	PutLogSegmentRecUsec               bucketstats.BucketLog2Round
	DeleteLogSegmentRecUsec            bucketstats.BucketLog2Round
	IndexedLogSegmentNumberUsec        bucketstats.BucketLog2Round
	GetBPlusTreeObjectUsec             bucketstats.BucketLog2Round
	GetBPlusTreeObjectBytes            bucketstats.BucketLog2Round
	PutBPlusTreeObjectUsec             bucketstats.BucketLog2Round
	PutBPlusTreeObjectBytes            bucketstats.BucketLog2Round
	DeleteBPlusTreeObjectUsec          bucketstats.BucketLog2Round
	IndexedBPlusTreeObjectNumberUsec   bucketstats.BucketLog2Round
	DoCheckpointUsec                   bucketstats.BucketLog2Round
	DaemonPerCheckpointUsec            bucketstats.BucketLog2Round
	DaemonPerCheckpointLockWaitUsec    bucketstats.BucketLog2Round
	DaemonPerCheckpointLockedUsec      bucketstats.BucketLog2Round
	DaemonPerCheckpointStatsUpdateUsec bucketstats.BucketLog2Round

	PutCheckpointUsec                   bucketstats.BucketLog2Round
	PutCheckpointBytes                  bucketstats.BucketLog2Round
	PutCheckpointInodeRecUsec           bucketstats.BucketLog2Round
	PutCheckpointInodeRecBytes          bucketstats.BucketLog2Round
	PutCheckpointInodeRecNodes          bucketstats.BucketLog2Round
	PutCheckpointLogSegmentUsec         bucketstats.BucketLog2Round
	PutCheckpointLogSegmentBytes        bucketstats.BucketLog2Round
	PutCheckpointLogSegmentNodes        bucketstats.BucketLog2Round
	PutCheckpointbPlusTreeObjectUsec    bucketstats.BucketLog2Round
	PutCheckpointbPlusTreeObjectBytes   bucketstats.BucketLog2Round
	PutCheckpointbPlusTreeObjectNodes   bucketstats.BucketLog2Round
	PutCheckpointSnapshotFlushUsec      bucketstats.BucketLog2Round
	PutCheckpointTreeLayoutUsec         bucketstats.BucketLog2Round
	PutCheckpointTreeLayoutBytes        bucketstats.BucketLog2Round
	PutCheckpointCheckpointTrailerBytes bucketstats.BucketLog2Round
	PutCheckpointSnapshotListUsec       bucketstats.BucketLog2Round
	PutCheckpointSnapshotListBytes      bucketstats.BucketLog2Round
	PutCheckpointChunkedPutUsec         bucketstats.BucketLog2Round
	PutCheckpointPostAndEtcdUsec        bucketstats.BucketLog2Round
	PutCheckpointObjectCleanupUsec      bucketstats.BucketLog2Round

	FetchLayoutReportUsec                     bucketstats.BucketLog2Round
	SnapShotCreateByInodeLayerUsec            bucketstats.BucketLog2Round
	SnapShotDeleteByInodeLayerUsec            bucketstats.BucketLog2Round
	SnapShotCountUsec                         bucketstats.BucketLog2Round
	SnapShotLookupByNameUsec                  bucketstats.BucketLog2Round
	SnapShotListByIDUsec                      bucketstats.BucketLog2Round
	SnapShotListByTimeUsec                    bucketstats.BucketLog2Round
	SnapShotListByNameUsec                    bucketstats.BucketLog2Round
	SnapShotU64DecodeUsec                     bucketstats.BucketLog2Round
	SnapShotIDAndNonceEncodeUsec              bucketstats.BucketLog2Round
	SnapShotTypeDotSnapShotAndNonceEncodeUsec bucketstats.BucketLog2Round

	GetInodeRecErrors                  bucketstats.Total
	PutInodeRecErrors                  bucketstats.Total
	PutInodeRecsErrors                 bucketstats.Total
	DeleteInodeRecErrors               bucketstats.Total
	IndexedInodeNumberErrors           bucketstats.Total
	NextInodeNumberErrors              bucketstats.Total
	GetLogSegmentRecErrors             bucketstats.Total
	PutLogSegmentRecErrors             bucketstats.Total
	DeleteLogSegmentRecErrors          bucketstats.Total
	IndexedLogSegmentNumberErrors      bucketstats.Total
	GetBPlusTreeObjectErrors           bucketstats.Total
	PutBPlusTreeObjectErrors           bucketstats.Total
	DeleteBPlusTreeObjectErrors        bucketstats.Total
	IndexedBPlusTreeObjectNumberErrors bucketstats.Total
	DoCheckpointErrors                 bucketstats.Total
	PutCheckpointErrors                bucketstats.Total
	FetchLayoutReportErrors            bucketstats.Total
	SnapShotCreateByInodeLayerErrors   bucketstats.Total
	SnapShotDeleteByInodeLayerErrors   bucketstats.Total
	SnapShotCountErrors                bucketstats.Total
	SnapShotLookupByNameErrors         bucketstats.Total
}

var globals globalsStruct

func init() {
	transitions.Register("headhunter", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	var (
		bPlusTreeObjectCacheEvictHighLimit       uint64
		bPlusTreeObjectCacheEvictLowLimit        uint64
		createdDeletedObjectsCacheEvictHighLimit uint64
		createdDeletedObjectsCacheEvictLowLimit  uint64
		dummyElementOfBPlusTreeLayoutStruct      ElementOfBPlusTreeLayoutStruct
		dummyReplayLogTransactionFixedPartStruct replayLogTransactionFixedPartStruct
		dummyUint64                              uint64
		inodeRecCacheEvictHighLimit              uint64
		inodeRecCacheEvictLowLimit               uint64
		logSegmentRecCacheEvictHighLimit         uint64
		logSegmentRecCacheEvictLowLimit          uint64
		mountRetryDelay                          time.Duration
		mountRetryExpBackoff                     float64
		mountRetryIndex                          uint16
		nextMountRetryDelay                      time.Duration
	)

	bucketstats.Register("proxyfs.headhunter", "", &globals)

	// Pre-compute crc64 ECMA Table & useful cstruct sizes

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	globals.uint64Size, _, err = cstruct.Examine(dummyUint64)
	if nil != err {
		return
	}

	globals.ElementOfBPlusTreeLayoutStructSize, _, err = cstruct.Examine(dummyElementOfBPlusTreeLayoutStruct)
	if nil != err {
		return
	}

	globals.replayLogTransactionFixedPartStructSize, _, err = cstruct.Examine(dummyReplayLogTransactionFixedPartStruct)
	if nil != err {
		return
	}

	// Initialize globals.volume{|Group}Map's

	globals.volumeGroupMap = make(map[string]*volumeGroupStruct)
	globals.volumeMap = make(map[string]*volumeStruct)

	// Initialize B+Tree caches

	inodeRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	inodeRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.inodeRecCache = sortedmap.NewBPlusTreeCache(inodeRecCacheEvictLowLimit, inodeRecCacheEvictHighLimit)

	globals.inodeRecCachePriorCacheHits = 0
	globals.inodeRecCachePriorCacheMisses = 0

	logSegmentRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	logSegmentRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.logSegmentRecCache = sortedmap.NewBPlusTreeCache(logSegmentRecCacheEvictLowLimit, logSegmentRecCacheEvictHighLimit)

	globals.logSegmentRecCachePriorCacheHits = 0
	globals.logSegmentRecCachePriorCacheMisses = 0

	bPlusTreeObjectCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictLowLimit")
	if nil != err {
		return
	}
	bPlusTreeObjectCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.bPlusTreeObjectCache = sortedmap.NewBPlusTreeCache(bPlusTreeObjectCacheEvictLowLimit, bPlusTreeObjectCacheEvictHighLimit)

	globals.bPlusTreeObjectCachePriorCacheHits = 0
	globals.bPlusTreeObjectCachePriorCacheMisses = 0

	createdDeletedObjectsCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictLowLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictLowLimit = logSegmentRecCacheEvictLowLimit // TODO: Eventually just return
	}
	createdDeletedObjectsCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictHighLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictHighLimit = logSegmentRecCacheEvictHighLimit // TODO: Eventually just return
	}

	globals.createdDeletedObjectsCache = sortedmap.NewBPlusTreeCache(createdDeletedObjectsCacheEvictLowLimit, createdDeletedObjectsCacheEvictHighLimit)

	globals.createdDeletedObjectsCachePriorCacheHits = 0
	globals.createdDeletedObjectsCachePriorCacheMisses = 0

	// Record mount retry parameters and compute retry delays

	globals.checkpointHeaderConsensusAttempts, err = confMap.FetchOptionValueUint16("FSGlobals", "CheckpointHeaderConsensusAttempts")
	if nil != err {
		globals.checkpointHeaderConsensusAttempts = 5 // TODO: Eventually just return
	}

	globals.mountRetryLimit, err = confMap.FetchOptionValueUint16("FSGlobals", "MountRetryLimit")
	if nil != err {
		globals.mountRetryLimit = 6 // TODO: Eventually just return
	}
	mountRetryDelay, err = confMap.FetchOptionValueDuration("FSGlobals", "MountRetryDelay")
	if nil != err {
		mountRetryDelay = time.Duration(1 * time.Second) // TODO: Eventually just return
	}
	mountRetryExpBackoff, err = confMap.FetchOptionValueFloat64("FSGlobals", "MountRetryExpBackoff")
	if nil != err {
		mountRetryExpBackoff = 2 // TODO: Eventually just return
	}

	globals.mountRetryDelay = make([]time.Duration, globals.mountRetryLimit)

	nextMountRetryDelay = mountRetryDelay

	for mountRetryIndex = 0; mountRetryIndex < globals.mountRetryLimit; mountRetryIndex++ {
		globals.mountRetryDelay[mountRetryIndex] = nextMountRetryDelay
		nextMountRetryDelay = time.Duration(float64(nextMountRetryDelay) * mountRetryExpBackoff)
	}

	// Fetch CheckpointHeader logging setting

	globals.logCheckpointHeaderPosts, err = confMap.FetchOptionValueBool("FSGlobals", "LogCheckpointHeaderPosts")
	if nil != err {
		globals.logCheckpointHeaderPosts = true // TODO: Eventually just return
	}

	// Record etcd parameters

	globals.etcdEnabled, err = confMap.FetchOptionValueBool("FSGlobals", "EtcdEnabled")
	if nil != err {
		globals.etcdEnabled = false // Current default
	}

	if globals.etcdEnabled {
		globals.etcdEndpoints, err = confMap.FetchOptionValueStringSlice("FSGlobals", "EtcdEndpoints")
		if nil != err {
			return
		}
		globals.etcdAutoSyncInterval, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdAutoSyncInterval")
		if nil != err {
			return
		}
		globals.etcdDialTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdDialTimeout")
		if nil != err {
			return
		}
		globals.etcdOpTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdOpTimeout")
		if nil != err {
			return
		}

		// Initialize etcd Client & KV objects

		globals.etcdClient, err = etcd.New(etcd.Config{
			Endpoints:        globals.etcdEndpoints,
			AutoSyncInterval: globals.etcdAutoSyncInterval,
			DialTimeout:      globals.etcdDialTimeout,
		})
		if nil != err {
			return
		}

		globals.etcdKV = etcd.NewKV(globals.etcdClient)
	}

	// Record MetadataRecycleBin setting

	globals.metadataRecycleBin, err = confMap.FetchOptionValueBool("FSGlobals", "MetadataRecycleBin")
	if nil != err {
		globals.metadataRecycleBin = false // TODO: Eventually just return or, perhaps, set to true
	}
	if globals.metadataRecycleBin {
		globals.metadataRecycleBinHeader = make(map[string][]string)
		globals.metadataRecycleBinHeader[MetadataRecycleBinHeaderName] = []string{MetadataRecycleBinHeaderValue}
	}

	// Start off with background object deletions enabled

	globals.backgroundObjectDeleteEnabled = true

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	volumeGroup := &volumeGroupStruct{name: volumeGroupName, volumeMap: make(map[string]*volumeStruct)}
	globals.Lock()
	_, ok := globals.volumeGroupMap[volumeGroupName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeGroupCreated() called for preexisting VolumeGroup (%s)", volumeGroupName)
		return
	}
	globals.volumeGroupMap[volumeGroupName] = volumeGroup
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	globals.Lock()
	_, ok := globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeGroupMoved() called for nonexistent VolumeGroup (%s)", volumeGroupName)
		return
	}
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	globals.Lock()
	volumeGroup, ok := globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeGroupDestroyed() called for nonexistent VolumeGroup (%s)", volumeGroupName)
		return
	}
	if 0 != len(volumeGroup.volumeMap) {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeGroupDestroyed() called for non-empty VolumeGroup (%s)", volumeGroupName)
		return
	}
	delete(globals.volumeGroupMap, volumeGroupName)
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	volume := &volumeStruct{volumeName: volumeName, served: false, checkpointTriggeringEvents: 0}
	globals.Lock()
	volumeGroup, ok := globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeCreated() called for Volume (%s) to be added to nonexistent VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}
	_, ok = globals.volumeMap[volumeName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeCreated() called for preexiting Volume (%s)", volumeName)
		return
	}
	_, ok = volumeGroup.volumeMap[volumeName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeCreated() called for preexiting Volume (%s) to be added to VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}
	volume.volumeGroup = volumeGroup
	volumeGroup.volumeMap[volumeName] = volume
	globals.volumeMap[volumeName] = volume
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	globals.Lock()
	volume, ok := globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeMoved() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeMoved() called for Volume (%s) being actively served", volumeName)
		return
	}
	newVolumeGroup, ok := globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeMoved() called for Volume (%s) to be moved to nonexistent VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}
	_, ok = newVolumeGroup.volumeMap[volumeName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeMoved() called for Volume (%s) to be moved to VolumeGroup (%s) already containing the Volume", volumeName, volumeGroupName)
		return
	}
	oldVolumeGroup := volume.volumeGroup
	delete(oldVolumeGroup.volumeMap, volumeName)
	newVolumeGroup.volumeMap[volumeName] = volume
	volume.volumeGroup = newVolumeGroup
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	globals.Lock()
	volume, ok := globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeDestroyed() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("headhunter.VolumeDestroyed() called for Volume (%s) being actively served", volumeName)
		return
	}
	delete(volume.volumeGroup.volumeMap, volumeName)
	delete(globals.volumeMap, volumeName)
	globals.Unlock()
	err = nil
	return
}

func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	globals.Lock()
	volume, ok := globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.ServeVolume() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("headhunter.ServeVolume() called for Volume (%s) already being served", volumeName)
		return
	}
	volume.served = true
	globals.Unlock()
	err = volume.up(confMap)
	if nil != err {
		err = fmt.Errorf("headhunter.ServeVolume() failed to \"up\" Volume (%s): %v", volumeName, err)
		return
	}
	err = nil
	return
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	globals.Lock()
	volume, ok := globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("headhunter.UnserveVolume() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if !volume.served {
		globals.Unlock()
		err = fmt.Errorf("headhunter.UnserveVolume() called for Volume (%s) not being served", volumeName)
		return
	}
	volume.served = false
	globals.Unlock()
	err = volume.down(confMap)
	if nil != err {
		err = fmt.Errorf("headhunter.UnserveVolume() failed to \"down\" Volume (%s): %v", volumeName, err)
		return
	}
	err = nil
	return
}

func (dummy *globalsStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	EnableObjectDeletions() // Otherwise, we will hang
	return nil
}
func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if globals.etcdEnabled {
		globals.etcdKV = nil

		err = globals.etcdClient.Close()
		if nil != err {
			return
		}
	}

	if 0 != len(globals.volumeGroupMap) {
		err = fmt.Errorf("headhunter.Down() called with 0 != len(globals.volumeGroupMap")
		return
	}
	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("headhunter.Down() called with 0 != len(globals.volumeMap")
		return
	}

	bucketstats.UnRegister("proxyfs.headhunter", "")

	err = nil
	return
}

func (volume *volumeStruct) up(confMap conf.ConfMap) (err error) {
	var (
		autoFormat            bool
		autoFormatStringSlice []string
		mountRetryIndex       uint16
		volumeSectionName     string
	)

	volumeSectionName = "Volume:" + volume.volumeName

	volume.checkpointChunkedPutContext = nil
	volume.eventListeners = make(map[VolumeEventListener]struct{})
	volume.checkpointRequestChan = make(chan *checkpointRequestStruct, 1)
	volume.postponePriorViewCreatedObjectsPuts = false
	volume.postponedPriorViewCreatedObjectsPuts = make(map[uint64]struct{})

	volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
	if nil != err {
		return
	}

	volume.maxFlushSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxFlushSize")
	if nil != err {
		return
	}

	volume.nonceValuesToReserve, err = confMap.FetchOptionValueUint16(volumeSectionName, "NonceValuesToReserve")
	if nil != err {
		return
	}

	volume.maxInodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxInodesPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxLogSegmentsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxLogSegmentsPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxDirFileNodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxDirFileNodesPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxCreatedDeletedObjectsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxCreatedDeletedObjectsPerMetadataNode")
	if nil != err {
		volume.maxCreatedDeletedObjectsPerMetadataNode = volume.maxLogSegmentsPerMetadataNode // TODO: Eventually just return
	}

	if globals.etcdEnabled {
		volume.checkpointEtcdKeyName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointEtcdKeyName")
		if nil != err {
			return
		}
	}

	volume.checkpointContainerName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerName")
	if nil != err {
		return
	}

	volume.checkpointContainerStoragePolicy, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerStoragePolicy")
	if nil != err {
		return
	}

	volume.checkpointInterval, err = confMap.FetchOptionValueDuration(volumeSectionName, "CheckpointInterval")
	if nil != err {
		return
	}

	volume.replayLogFileName, err = confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
	if nil == err {
		// Provision aligned buffer used to write to Replay Log
		volume.defaultReplayLogWriteBuffer = constructReplayLogWriteBuffer(replayLogWriteBufferDefaultSize)
	} else {
		// Disable Replay Log
		volume.replayLogFileName = ""
	}

	volume.snapShotIDNumBits, err = confMap.FetchOptionValueUint16(volumeSectionName, "SnapShotIDNumBits")
	if nil != err {
		volume.snapShotIDNumBits = 10 // TODO: Eventually just return
	}
	if 2 > volume.snapShotIDNumBits {
		err = fmt.Errorf("[%v]SnapShotIDNumBits must be at least 2", volumeSectionName)
		return
	}
	if 32 < volume.snapShotIDNumBits {
		err = fmt.Errorf("[%v]SnapShotIDNumBits must be no more than 32", volumeSectionName)
		return
	}

	autoFormatStringSlice, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "AutoFormat")
	if nil == err {
		if 1 != len(autoFormatStringSlice) {
			err = fmt.Errorf("If specified, [%v]AutoFormat must be single-valued and either true or false", volumeSectionName)
			return
		}
		autoFormat, err = confMap.FetchOptionValueBool(volumeSectionName, "AutoFormat")
		if nil != err {
			return
		}
	} else {
		autoFormat = false // Default to false if not present
	}

	err = volume.getCheckpoint(autoFormat)
	if nil != err {
		logger.Warnf("Initial attempt to mount Volume %s failed: %v", volume.volumeName, err)
		mountRetryIndex = 0
		for {
			if mountRetryIndex == globals.mountRetryLimit {
				err = fmt.Errorf("MountRetryLimit (%d) for Volume %s exceeded", globals.mountRetryLimit, volume.volumeName)
				return
			}

			time.Sleep(globals.mountRetryDelay[mountRetryIndex])

			err = volume.getCheckpoint(autoFormat)
			if nil == err {
				break
			}

			mountRetryIndex++ // Pre-increment to identify first retry as #1 in following logger.Warnf() call

			logger.Warnf("Mount Retry #%d failed: %v", mountRetryIndex, err)
		}
	}

	go volume.checkpointDaemon()

	err = nil
	return
}

func (volume *volumeStruct) down(confMap conf.ConfMap) (err error) {
	var (
		checkpointRequest checkpointRequestStruct
	)

	checkpointRequest.exitOnCompletion = true
	checkpointRequest.waitGroup.Add(1)

	volume.checkpointRequestChan <- &checkpointRequest

	checkpointRequest.waitGroup.Wait()

	err = checkpointRequest.err

	volume.backgroundObjectDeleteWG.Wait()

	return
}
