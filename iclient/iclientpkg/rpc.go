// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/NVIDIA/proxyfs/iauth"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

func startRPCHandler() (err error) {
	var (
		customTransport  *http.Transport
		defaultTransport *http.Transport
		mountRequest     *imgrpkg.MountRequestStruct
		mountResponse    *imgrpkg.MountResponseStruct
		ok               bool
	)

	defaultTransport, ok = http.DefaultTransport.(*http.Transport)
	if !ok {
		err = fmt.Errorf("http.DefaultTransport.(*http.Transport) returned !ok\n")
		return
	}

	customTransport = &http.Transport{ // Up-to-date as of Golang 1.11
		Proxy:                  defaultTransport.Proxy,
		DialContext:            defaultTransport.DialContext,
		Dial:                   defaultTransport.Dial,
		DialTLS:                defaultTransport.DialTLS,
		TLSClientConfig:        defaultTransport.TLSClientConfig,
		TLSHandshakeTimeout:    globals.config.SwiftTimeout,
		DisableKeepAlives:      false,
		DisableCompression:     defaultTransport.DisableCompression,
		MaxIdleConns:           int(globals.config.SwiftConnectionPoolSize),
		MaxIdleConnsPerHost:    int(globals.config.SwiftConnectionPoolSize),
		MaxConnsPerHost:        int(globals.config.SwiftConnectionPoolSize),
		IdleConnTimeout:        globals.config.SwiftTimeout,
		ResponseHeaderTimeout:  globals.config.SwiftTimeout,
		ExpectContinueTimeout:  globals.config.SwiftTimeout,
		TLSNextProto:           defaultTransport.TLSNextProto,
		ProxyConnectHeader:     defaultTransport.ProxyConnectHeader,
		MaxResponseHeaderBytes: defaultTransport.MaxResponseHeaderBytes,
	}

	globals.httpClient = &http.Client{
		Transport: customTransport,
		Timeout:   globals.config.SwiftTimeout,
	}

	if globals.config.AuthPlugInEnvName == "" {
		globals.swiftAuthInString = globals.config.AuthPlugInEnvValue
	} else {
		globals.swiftAuthInString = os.Getenv(globals.config.AuthPlugInEnvName)
	}

	updateSwithAuthTokenAndSwiftStorageURL()

	globals.retryRPCClientConfig = &retryrpc.ClientConfig{
		DNSOrIPAddr:              globals.config.RetryRPCPublicIPAddr,
		Port:                     int(globals.config.RetryRPCPort),
		RootCAx509CertificatePEM: globals.retryRPCCACertPEM,
		Callbacks:                &globals,
		DeadlineIO:               globals.config.RetryRPCDeadlineIO,
		KeepAlivePeriod:          globals.config.RetryRPCKeepAlivePeriod,
	}

	globals.retryRPCClient, err = retryrpc.NewClient(globals.retryRPCClientConfig)
	if nil != err {
		return
	}

	mountRequest = &imgrpkg.MountRequestStruct{
		VolumeName: globals.config.VolumeName,
		AuthToken:  fetchSwiftAuthToken(),
	}
	mountResponse = &imgrpkg.MountResponseStruct{}

	err = globals.retryRPCClient.Send("Mount", mountRequest, mountResponse)
	if nil != err {
		return
	}

	globals.mountID = mountResponse.MountID

	err = nil
	return
}

func renewRPCHandler() (err error) {
	var (
		renewMountRequest  *imgrpkg.RenewMountRequestStruct
		renewMountResponse *imgrpkg.RenewMountResponseStruct
	)

	updateSwithAuthTokenAndSwiftStorageURL()

	renewMountRequest = &imgrpkg.RenewMountRequestStruct{
		MountID:   globals.mountID,
		AuthToken: fetchSwiftAuthToken(),
	}
	renewMountResponse = &imgrpkg.RenewMountResponseStruct{}

	err = globals.retryRPCClient.Send("RenewMount", renewMountRequest, renewMountResponse)

	return // err, as set by globals.retryRPCClient.Send("RenewMount", renewMountRequest, renewMountResponse) is sufficient
}

func stopRPCHandler() (err error) {
	var (
		unmountRequest  *imgrpkg.UnmountRequestStruct
		unmountResponse *imgrpkg.UnmountResponseStruct
	)

	unmountRequest = &imgrpkg.UnmountRequestStruct{
		MountID: globals.mountID,
	}
	unmountResponse = &imgrpkg.UnmountResponseStruct{}

	err = globals.retryRPCClient.Send("Unmount", unmountRequest, unmountResponse)
	if nil != err {
		logWarn(err)
	}

	globals.retryRPCClient.Close()

	globals.httpClient = nil

	globals.retryRPCClientConfig = nil
	globals.retryRPCClient = nil

	err = nil
	return
}

func fetchSwiftAuthToken() (swiftAuthToken string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			globals.Unlock()
			return
		}

		globals.Lock()

		swiftAuthWaitGroup.Wait()
	}
}

func fetchSwiftStorageURL() (swiftStorageURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftStorageURL = globals.swiftStorageURL
			globals.Unlock()
			return
		}

		globals.Lock()

		swiftAuthWaitGroup.Wait()
	}
}

func updateSwithAuthTokenAndSwiftStorageURL() {
	var (
		err                error
		swiftAuthWaitGroup *sync.WaitGroup
	)

	globals.Lock()

	swiftAuthWaitGroup = globals.swiftAuthWaitGroup

	if nil != swiftAuthWaitGroup {
		globals.Unlock()
		swiftAuthWaitGroup.Wait()
		return
	}

	globals.swiftAuthWaitGroup = &sync.WaitGroup{}
	globals.swiftAuthWaitGroup.Add(1)

	globals.Unlock()

	globals.swiftAuthToken, globals.swiftStorageURL, err = iauth.PerformAuth(globals.config.AuthPlugInPath, globals.swiftAuthInString)
	if nil != err {
		logFatalf("iauth.PerformAuth() failed: %v", err)
	}

	globals.Lock()
	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil
	globals.Unlock()
}
