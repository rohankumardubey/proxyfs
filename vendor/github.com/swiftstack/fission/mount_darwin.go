package fission

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

const (
	devOsxFusePathPattern = "/dev/osxfuse*" // Suffix is a non-negative decimal number starting with 0, 1, ...
	osxFuseLoadPath       = "/Library/Filesystems/osxfuse.fs/Contents/Resources/load_osxfuse"
	osxFuseMountPath      = "/Library/Filesystems/osxfuse.fs/Contents/Resources/mount_osxfuse"
	osxFuseMountCallByEnv = "MOUNT_OSXFUSE_CALL_BY_LIB=" // No value should be appended
	osxFuseDaemonPathEnv  = "MOUNT_OSXFUSE_DAEMON_PATH=" // Append program name (os.Args[0])
)

func (volume *volumeStruct) DoMount() (err error) {
	var (
		allowOtherOption              string
		devOsxFusePath                string
		devOsxFusePathList            []string
		fsnameOption                  string
		iosizeOption                  string
		localOption                   string
		mountOptions                  string
		noAppleDoubleOption           string
		noAppleXattrOption            string
		osxFuseLoadCmd                *exec.Cmd
		osxFuseLoadCmdCombinedOutput  []byte
		osxFuseMountCmd               *exec.Cmd
		osxFuseMountCmdCombinedOutput []byte
		volnameOption                 string
	)

	// Ensure OSXFuse is installed

	_, err = os.Stat(osxFuseLoadPath)
	if nil != err {
		volume.logger.Printf("DoMount() unable to find osxFuseLoadPath (\"%s\"): %v", osxFuseLoadPath, err)
		return
	}
	_, err = os.Stat(osxFuseMountPath)
	if nil != err {
		volume.logger.Printf("DoMount() unable to find osxFuseMountPath (\"%s\"): %v", osxFuseMountPath, err)
		return
	}

	// Ensure OSXFuse is loaded

	devOsxFusePathList, err = filepath.Glob(devOsxFusePathPattern)
	if nil != err {
		volume.logger.Printf("DoMount() unable to issue filepath.Glob(devOsxFusePathPattern=\"%s\"): %v [Case 1]", devOsxFusePathPattern, err)
		return
	}

	if 0 == len(devOsxFusePathList) {
		// OSXFuse must not be loaded yet

		osxFuseLoadCmd = exec.Command(osxFuseLoadPath)
		osxFuseLoadCmd.Dir = "/" // Not sure if this is necessary

		osxFuseLoadCmdCombinedOutput, err = osxFuseLoadCmd.CombinedOutput()
		if nil != err {
			volume.logger.Printf("DoMount() unable to load OSXFuse via osxFuseLoadPath (\"%s\") [%v]: %s", osxFuseLoadPath, err, string(osxFuseLoadCmdCombinedOutput[:]))
			return
		}

		// Re-fetch devOsxFusePathList

		devOsxFusePathList, err = filepath.Glob(devOsxFusePathPattern)
		if nil != err {
			volume.logger.Printf("DoMount() unable to issue filepath.Glob(devOsxFusePathPattern=\"%s\"): %v [Case 2]", devOsxFusePathPattern, err)
			return
		}
	}

	// Compute mountOptions

	allowOtherOption = "allow_other"
	localOption = "local"
	noAppleDoubleOption = "noappledouble"
	noAppleXattrOption = "noapplexattr"
	fsnameOption = "fsname=" + volume.volumeName
	volnameOption = "volname=" + volume.volumeName

	iosizeOption = fmt.Sprintf("iosize=%d", volume.initOutMaxWrite)

	mountOptions = allowOtherOption +
		"," + localOption +
		"," + noAppleDoubleOption +
		"," + noAppleXattrOption +
		"," + fsnameOption +
		"," + volnameOption +
		"," + iosizeOption

	// Find an available FUSE device file

	for _, devOsxFusePath = range devOsxFusePathList {
		volume.devFuseFile, err = os.OpenFile(devOsxFusePath, os.O_RDWR, 0000)
		if nil != err {
			// Not this one... must be busy
			continue
		}
		volume.devFuseFD = int(volume.devFuseFile.Fd())

		// Mount via this FUSE device file using Mount Helper (fusermount equivalent)

		volume.devFuseFDReaderWG.Add(1)
		go volume.devFuseFDReader()

		osxFuseMountCmd = &exec.Cmd{
			Path: osxFuseMountPath,
			Args: []string{
				osxFuseMountPath,
				"-o", mountOptions,
				"3", // First ExtraFiles should be an *os.File of volume.devFuseFD
				volume.mountpointDirPath,
			},
			Env:          append(os.Environ(), osxFuseMountCallByEnv, osxFuseDaemonPathEnv+os.Args[0]),
			Dir:          "",
			Stdin:        nil,
			Stdout:       nil, // This will be redirected to osxFuseMountCmdCombinedOutput below
			Stderr:       nil, // This will be redirected to osxFuseMountCmdCombinedOutput below
			ExtraFiles:   []*os.File{volume.devFuseFile},
			SysProcAttr:  nil,
			Process:      nil,
			ProcessState: nil,
		}

		osxFuseMountCmdCombinedOutput, err = osxFuseMountCmd.CombinedOutput()
		if nil != err {
			volume.logger.Printf("DoMount() unable to mount %s (%v): %s", volume.volumeName, err, string(osxFuseMountCmdCombinedOutput[:]))
			return
		}

		volume.logger.Printf("Volume %s mounted on mountpoint %s", volume.volumeName, volume.mountpointDirPath)

		return
	}

	// If we reach here, no available FUSE device files

	volume.logger.Printf("DoMount() unable to find available FUSE device file")

	return
}

func (volume *volumeStruct) DoUnmount() (err error) {
	err = syscall.Unmount(volume.mountpointDirPath, 0)
	if nil != err {
		volume.logger.Printf("DoUnmount() unable to unmount volume %s from mountpoint %s: %v", volume.volumeName, volume.mountpointDirPath, err)
		return
	}

	err = syscall.Close(volume.devFuseFD)
	if nil != err {
		volume.logger.Printf("DoUnmount() unable to close /dev/osxfuse*: %v", err)
		return
	}

	volume.devFuseFDReaderWG.Wait()

	volume.logger.Printf("Volume %s unmounted from mountpoint %s", volume.volumeName, volume.mountpointDirPath)

	err = nil
	return
}
