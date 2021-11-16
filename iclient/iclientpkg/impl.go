// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

func start(confMap conf.ConfMap, fissionErrChan chan error) (err error) {
	err = initializeGlobals(confMap, fissionErrChan)
	if nil != err {
		return
	}

	err = startRPCHandler()
	if nil != err {
		return
	}

	err = startLeaseHandler()
	if nil != err {
		return
	}

	err = performMountFUSE()
	if nil != err {
		return
	}

	err = startHTTPServer()
	if nil != err {
		return
	}

	return
}

func stop() (err error) {
	err = stopHTTPServer()
	if nil != err {
		return
	}

	err = performUnmountFUSE()
	if nil != err {
		return
	}

	err = stopLeaseHandler()
	if nil != err {
		return
	}

	err = stopRPCHandler()
	if nil != err {
		return
	}

	err = uninitializeGlobals()

	return
}

func signal() (err error) {
	logSIGHUP()

	err = nil
	return
}
