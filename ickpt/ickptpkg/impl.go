// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ickptpkg

import (
	"container/list"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/conf"
)

const (
	startHEADMaxRetries = 10
	startHEADRetryDelay = 100 * time.Millisecond
)

type configStruct struct {
	IPAddr                  string
	Port                    uint16
	CertFilePath            string
	KeyFilePath             string
	CACertFilePath          string
	DataBasePath            string
	SwiftTimeout            time.Duration
	SwiftConnectionPoolSize uint32
	TransactionTimeout      time.Duration
}

type canonicalStorageUrlTransactionStruct struct {
	canonicalStorageUrl string
	path                string
	isPut               bool
	startTime           time.Time
	listElement         *list.Element
}

type globalsStruct struct {
	sync.Mutex
	sync.WaitGroup
	config                             configStruct
	servingTLS                         bool
	httpClient                         *http.Client //          used both in startService() and to talk to Swift
	httpServer                         *http.Server
	canonicalStorageUrlWGListMap       map[string]*list.List                            // key == locked canonicalStorageUrl; value == list of sync.WaitGroup's
	canonicalStorageUrlTransactionMap  map[string]*canonicalStorageUrlTransactionStruct // key == canonicalStorageUrl
	canonicalStorageUrlTransactionList *list.List                                       // *canonicalStorageUrlTranactionStruct LRU
}

var globals globalsStruct

func start(confMap conf.ConfMap) (err error) {
	err = initializeGlobals(confMap)
	if err != nil {
		return
	}

	err = os.MkdirAll(globals.config.DataBasePath, 0666)
	if err != nil {
		return
	}

	err = startService()
	if err != nil {
		return
	}

	return
}

func stop() (err error) {
	err = stopService()
	if err != nil {
		return
	}

	uninitializeGlobals()

	err = nil
	return
}

func initializeGlobals(confMap conf.ConfMap) (err error) {
	var (
		caCertFilePathMissingOrEmpty bool
		certFilePathMissingOrEmpty   bool
		customTransport              *http.Transport
		defaultTransport             *http.Transport
		keyFilePathMissingOrEmpty    bool
		ok                           bool
	)

	globals.config.IPAddr, err = confMap.FetchOptionValueString("ICKPT", "IPAddr")
	if err != nil {
		return
	}
	globals.config.Port, err = confMap.FetchOptionValueUint16("ICKPT", "Port")
	if err != nil {
		return
	}

	err = confMap.VerifyOptionIsMissing("ICKPT", "CertFilePath")
	if err == nil {
		certFilePathMissingOrEmpty = true
	} else {
		err = confMap.VerifyOptionValueIsEmpty("ICKPT", "CertFilePath")
		if err == nil {
			certFilePathMissingOrEmpty = true
		} else {
			certFilePathMissingOrEmpty = false
		}
	}

	err = confMap.VerifyOptionIsMissing("ICKPT", "KeyFilePath")
	if err == nil {
		keyFilePathMissingOrEmpty = true
	} else {
		err = confMap.VerifyOptionValueIsEmpty("ICKPT", "KeyFilePath")
		if err == nil {
			keyFilePathMissingOrEmpty = true
		} else {
			keyFilePathMissingOrEmpty = false
		}
	}

	err = confMap.VerifyOptionIsMissing("ICKPT", "CACertFilePath")
	if err == nil {
		caCertFilePathMissingOrEmpty = true
	} else {
		err = confMap.VerifyOptionValueIsEmpty("ICKPT", "CACertFilePath")
		if err == nil {
			caCertFilePathMissingOrEmpty = true
		} else {
			caCertFilePathMissingOrEmpty = false
		}
	}

	if certFilePathMissingOrEmpty && keyFilePathMissingOrEmpty && caCertFilePathMissingOrEmpty {
		globals.servingTLS = false

		globals.config.CertFilePath = ""
		globals.config.KeyFilePath = ""
		globals.config.CACertFilePath = ""
	} else if !certFilePathMissingOrEmpty && !keyFilePathMissingOrEmpty && !caCertFilePathMissingOrEmpty {
		globals.servingTLS = true

		globals.config.CertFilePath, err = confMap.FetchOptionValueString("ICKPT", "CertFilePath")
		if err != nil {
			return
		}
		globals.config.KeyFilePath, err = confMap.FetchOptionValueString("ICKPT", "KeyFilePath")
		if err != nil {
			return
		}
		globals.config.CACertFilePath, err = confMap.FetchOptionValueString("ICKPT", "CACertFilePath")
		if err != nil {
			return
		}
	} else {
		err = fmt.Errorf("[ICKPT]{Cert|Key|CACert}FilePath must either all be present or all Emtpy/Mising")
		return
	}

	globals.config.DataBasePath, err = confMap.FetchOptionValueString("ICKPT", "DataBasePath")
	if err != nil {
		return
	}

	globals.config.SwiftTimeout, err = confMap.FetchOptionValueDuration("ICKPT", "SwiftTimeout")
	if err != nil {
		return
	}
	globals.config.SwiftConnectionPoolSize, err = confMap.FetchOptionValueUint32("ICKPT", "SwiftConnectionPoolSize")
	if err != nil {
		return
	}

	globals.config.TransactionTimeout, err = confMap.FetchOptionValueDuration("ICKPT", "TransactionTimeout")
	if err != nil {
		return
	}

	defaultTransport, ok = http.DefaultTransport.(*http.Transport)
	if !ok {
		err = fmt.Errorf("http.DefaultTransport.(*http.Transport) returned !ok")
		return
	}

	customTransport = &http.Transport{ // Up-to-date as of Golang 1.17
		Proxy:                  defaultTransport.Proxy,
		DialContext:            defaultTransport.DialContext,
		DialTLSContext:         defaultTransport.DialTLSContext,
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
		WriteBufferSize:        0,
		ReadBufferSize:         0,
	}

	globals.httpClient = &http.Client{
		Transport: customTransport,
		Timeout:   globals.config.SwiftTimeout,
	}

	globals.canonicalStorageUrlWGListMap = make(map[string]*list.List)

	globals.canonicalStorageUrlTransactionMap = make(map[string]*canonicalStorageUrlTransactionStruct)
	globals.canonicalStorageUrlTransactionList = list.New()

	err = nil
	return
}

func uninitializeGlobals() {
	globals.config.IPAddr = ""
	globals.config.Port = 0
	globals.config.CertFilePath = ""
	globals.config.KeyFilePath = ""
	globals.config.DataBasePath = ""
	globals.config.SwiftTimeout = time.Duration(0)
	globals.config.SwiftConnectionPoolSize = 0
	globals.config.TransactionTimeout = time.Duration(0)

	globals.httpClient = nil

	globals.canonicalStorageUrlWGListMap = nil

	globals.canonicalStorageUrlTransactionMap = nil
	globals.canonicalStorageUrlTransactionList = nil
}

func startService() (err error) {
	var (
		httpClient          *http.Client
		httpRequest         *http.Request
		httpResponse        *http.Response
		objectURL           string
		ok                  bool
		rootCA              []byte
		rootCAPool          *x509.CertPool
		startHEADNumRetries int
	)

	globals.httpServer = &http.Server{
		Addr:    net.JoinHostPort(globals.config.IPAddr, fmt.Sprintf("%d", globals.config.Port)),
		Handler: &globals,
	}

	if globals.servingTLS {
		rootCA, err = ioutil.ReadFile(globals.config.CACertFilePath)
		if err != nil {
			return
		}
		rootCAPool = x509.NewCertPool()
		ok = rootCAPool.AppendCertsFromPEM(rootCA)
		if !ok {
			err = fmt.Errorf("rootCAPool.AppendCertsFromPEM(rootCA) returned !ok")
			return
		}

		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: rootCAPool,
				},
			},
		}

		objectURL = "https://" + globals.httpServer.Addr + "/"
	} else {
		httpClient = &http.Client{}

		objectURL = "http://" + globals.httpServer.Addr + "/"
	}

	globals.Add(1)

	go func() {
		if globals.servingTLS {
			_ = globals.httpServer.ListenAndServeTLS(globals.config.CertFilePath, globals.config.KeyFilePath)
		} else {
			_ = globals.httpServer.ListenAndServe()
		}
		globals.Done()
	}()

	startHEADNumRetries = 0

	for {
		httpRequest, err = http.NewRequest("HEAD", objectURL, nil)
		if err == nil {
			httpResponse, err = httpClient.Do(httpRequest)
			if err == nil {
				_, err = ioutil.ReadAll(httpResponse.Body)
				if err == nil {
					err = httpResponse.Body.Close()
					if err == nil {
						if (httpResponse.StatusCode >= 200) && (httpResponse.StatusCode <= 299) {
							break
						}
					}
				}
			}
		}

		startHEADNumRetries++
		if startHEADNumRetries > startHEADMaxRetries {
			_ = stopService()
			err = fmt.Errorf("startService() failed to establish that service is up")
			return
		}

		time.Sleep(startHEADRetryDelay)
	}

	err = nil
	return
}

func stopService() (err error) {
	err = globals.httpServer.Close()
	if err != nil {
		return
	}

	globals.Wait()

	err = nil
	return
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// Branch off to individual request method handlers

	switch request.Method {
	case http.MethodDelete:
		doDELETE(responseWriter, request)
	case http.MethodGet:
		doGET(responseWriter, request)
	case http.MethodHead:
		doHEAD(responseWriter, request)
	case http.MethodPost:
		doPOST(responseWriter, request)
	case http.MethodPut:
		doPUT(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func swallowBody(request *http.Request) {
	var (
		err error
	)

	_, err = ioutil.ReadAll(request.Body)
	if err != nil {
		panic(fmt.Errorf("ioutil.ReadAll(request.Body) failed: %v", err))
	}

	err = request.Body.Close()
	if err != nil {
		panic(fmt.Errorf("request.Body.Close() failed: %v", err))
	}
}

func readBody(request *http.Request) (body []byte) {
	var (
		err error
	)

	body, err = ioutil.ReadAll(request.Body)
	if err != nil {
		panic(fmt.Errorf("ioutil.ReadAll(request.Body) failed: %v", err))
	}

	err = request.Body.Close()
	if err != nil {
		panic(fmt.Errorf("request.Body.Close() failed: %v", err))
	}

	return
}

func fetchCanonicalStorageUrl(request *http.Request) (key string) {
	var (
		xStorageUrl       string
		xStorageUrlScheme string
		xStorageUrlSplit  []string
	)

	xStorageUrl = request.Header.Get("X-Storage-Url")
	xStorageUrlSplit = strings.SplitN(xStorageUrl, "://", 2)

	if len(xStorageUrlSplit) != 2 {
		key = ""
		return
	}

	xStorageUrlScheme = strings.ToLower(xStorageUrlSplit[0])

	if (xStorageUrlScheme != "http") && (xStorageUrlScheme != "https") {
		key = ""
		return
	}

	key = base64.StdEncoding.EncodeToString([]byte(xStorageUrlSplit[1]))

	return
}

func lockCanonicalStorageUrl(canonicalStorageUrl string) {
	var (
		ok     bool
		wg     sync.WaitGroup
		wgList *list.List
	)

	globals.Lock()

	wgList, ok = globals.canonicalStorageUrlWGListMap[canonicalStorageUrl]
	if ok {
		// Somebody else has it locked... so indicate we want it and wait

		wg.Add(1)
		_ = wgList.PushBack(&wg)
		globals.Unlock()
		wg.Wait()

		// When we wake up, the prior lock holder has given it to us
	} else {
		// Nobody else has it locked... so just mark it locked (by us)

		globals.canonicalStorageUrlWGListMap[canonicalStorageUrl] = list.New()
		globals.Unlock()
	}
}

func unlockCanonicalStorageUrl(canonicalStorageUrl string) {
	var (
		ok            bool
		wg            *sync.WaitGroup
		wgList        *list.List
		wgListElement *list.Element
	)

	globals.Lock()

	wgList, ok = globals.canonicalStorageUrlWGListMap[canonicalStorageUrl]
	if !ok {
		panic(fmt.Errorf("globals.canonicalStorageUrlWGListMap[canonicalStorageUrl] returned !ok"))
	}

	if wgList.Len() == 0 {
		// Nobody waiting... so just mark it unlocked

		delete(globals.canonicalStorageUrlWGListMap, canonicalStorageUrl)
	} else {
		// At least one waiter... so give transfer lock to oldest one

		wgListElement = wgList.Front()
		wg = wgListElement.Value.(*sync.WaitGroup)
		wgList.Remove(wgListElement)
		wg.Done()
	}

	globals.Unlock()
}

func pruneCanonicalStorageUrlTransactionListWhileLocked() {
	var (
		canonicalStorageUrlTransaction *canonicalStorageUrlTransactionStruct
		err                            error
		listElement                    *list.Element
		ok                             bool
	)

	for {
		listElement = globals.canonicalStorageUrlTransactionList.Front()
		if globals.canonicalStorageUrlTransactionList.Front() == nil {
			break
		}

		canonicalStorageUrlTransaction, ok = listElement.Value.(*canonicalStorageUrlTransactionStruct)
		if !ok {
			panic(fmt.Errorf("listElement.Value.(*canonicalStorageUrlTransactionStruct) returned !ok"))
		}

		if time.Since(canonicalStorageUrlTransaction.startTime) < globals.config.TransactionTimeout {
			break
		}

		_, ok = globals.canonicalStorageUrlWGListMap[canonicalStorageUrlTransaction.canonicalStorageUrl]
		if ok {
			// In case it isn't "us" holding the canonicalStorageUrl lock, let's just exit

			break
		}

		if canonicalStorageUrlTransaction.isPut {
			err = os.Remove(globals.config.DataBasePath + "/" + canonicalStorageUrlTransaction.canonicalStorageUrl + "_next")
			if err != nil {
				panic(fmt.Errorf("os.Remove(globals.config.DataBasePath + \"/\" + canonicalStorageUrl + \"_next\") failed: %v", err))
			}
		}

		delete(globals.canonicalStorageUrlTransactionMap, canonicalStorageUrlTransaction.canonicalStorageUrl)
		globals.canonicalStorageUrlTransactionList.Remove(listElement)
	}
}

func beginCanonicalStorageUrlTransaction(canonicalStorageUrl string, path string, isPut bool) {
	var (
		canonicalStorageUrlTransaction *canonicalStorageUrlTransactionStruct
		err                            error
		ok                             bool
	)

	globals.Lock()

	pruneCanonicalStorageUrlTransactionListWhileLocked()

	canonicalStorageUrlTransaction, ok = globals.canonicalStorageUrlTransactionMap[canonicalStorageUrl]
	if ok {
		if canonicalStorageUrlTransaction.isPut {
			err = os.Remove(globals.config.DataBasePath + "/" + canonicalStorageUrl + "_next")
			if err != nil {
				panic(fmt.Errorf("os.Remove(globals.config.DataBasePath + \"/\" + canonicalStorageUrl + \"_next\") failed: %v", err))
			}
		}

		canonicalStorageUrlTransaction.path = path
		canonicalStorageUrlTransaction.isPut = isPut
		canonicalStorageUrlTransaction.startTime = time.Now()
		globals.canonicalStorageUrlTransactionList.MoveToBack(canonicalStorageUrlTransaction.listElement)
	} else {
		canonicalStorageUrlTransaction = &canonicalStorageUrlTransactionStruct{
			canonicalStorageUrl: canonicalStorageUrl,
			path:                path,
			isPut:               isPut,
			startTime:           time.Now(),
		}

		globals.canonicalStorageUrlTransactionMap[canonicalStorageUrl] = canonicalStorageUrlTransaction
		canonicalStorageUrlTransaction.listElement = globals.canonicalStorageUrlTransactionList.PushBack(canonicalStorageUrlTransaction)
	}

	globals.Unlock()
}

func fetchCanonicalStorageUrlTransaction(canonicalStorageUrl string) (canonicalStorageUrlTransaction *canonicalStorageUrlTransactionStruct, ok bool) {
	globals.Lock()

	canonicalStorageUrlTransaction, ok = globals.canonicalStorageUrlTransactionMap[canonicalStorageUrl]
	if !ok {
		panic(fmt.Errorf("globals.canonicalStorageUrlTransactionMap[canonicalStorageUrl] returned !ok"))
	}

	globals.Unlock()

	return
}

func (canonicalStorageUrlTransaction *canonicalStorageUrlTransactionStruct) end() {
	globals.Lock()

	delete(globals.canonicalStorageUrlTransactionMap, canonicalStorageUrlTransaction.canonicalStorageUrl)
	_ = globals.canonicalStorageUrlTransactionList.Remove(canonicalStorageUrlTransaction.listElement)

	pruneCanonicalStorageUrlTransactionListWhileLocked()

	globals.Unlock()
}

func swiftObjectGet(objectURL string, authToken string) (buf []byte, httpStatusCode int, err error) {
	var (
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	httpRequest, err = http.NewRequest("GET", objectURL, nil)
	if err != nil {
		return
	}

	if authToken != "" {
		httpRequest.Header["X-Auth-Token"] = []string{authToken}
	}

	httpResponse, err = globals.httpClient.Do(httpRequest)
	if err != nil {
		err = fmt.Errorf("globals.httpClient.Do(GET %s) failed: %v", objectURL, err)
		return
	}

	buf, err = ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
		return
	}
	err = httpResponse.Body.Close()
	if err != nil {
		err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
		return
	}

	httpStatusCode = httpResponse.StatusCode
	err = nil

	return
}

func doDELETE(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		canonicalStorageUrl string
		err                 error
		httpStatusCode      int
	)

	swallowBody(request)

	canonicalStorageUrl = fetchCanonicalStorageUrl(request)
	if canonicalStorageUrl == "" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	lockCanonicalStorageUrl(canonicalStorageUrl)

	_, httpStatusCode, err = swiftObjectGet(request.Header.Get("X-Storage-Url"), request.Header.Get("X-Auth-Token"))
	if (err != nil) || (httpStatusCode < 200) || (httpStatusCode > 299) {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		if err != nil {
			httpStatusCode = http.StatusInternalServerError
		}
		responseWriter.WriteHeader(httpStatusCode)
		return
	}

	beginCanonicalStorageUrlTransaction(canonicalStorageUrl, request.URL.Path, false)

	_ = os.Remove(globals.config.DataBasePath + "/" + canonicalStorageUrl + "_next")

	unlockCanonicalStorageUrl(canonicalStorageUrl)

	responseWriter.WriteHeader(http.StatusNoContent)
}

func doGET(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		canonicalStorageUrl string
		err                 error
		fileBody            []byte
		httpStatusCode      int
		objectBody          []byte
	)

	swallowBody(request)

	canonicalStorageUrl = fetchCanonicalStorageUrl(request)
	if canonicalStorageUrl == "" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	lockCanonicalStorageUrl(canonicalStorageUrl)

	objectBody, httpStatusCode, err = swiftObjectGet(request.Header.Get("X-Storage-Url"), request.Header.Get("X-Auth-Token"))
	if (err != nil) || (httpStatusCode < 200) || (httpStatusCode > 299) {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		if err != nil {
			httpStatusCode = http.StatusInternalServerError
		}
		responseWriter.WriteHeader(httpStatusCode)
		return
	}

	fileBody, err = os.ReadFile(globals.config.DataBasePath + "/" + canonicalStorageUrl)

	unlockCanonicalStorageUrl(canonicalStorageUrl)

	responseWriter.WriteHeader(http.StatusOK)

	if err == nil {
		_, err = responseWriter.Write(fileBody)
		if err != nil {
			panic(fmt.Errorf("responseWriter.Write(fileBody) failed: %v", err))
		}
	} else {
		_, err = responseWriter.Write(objectBody)
		if err != nil {
			panic(fmt.Errorf("responseWriter.Write(objectBody) failed: %v", err))
		}
	}
}

func doHEAD(responseWriter http.ResponseWriter, request *http.Request) {
	swallowBody(request)

	responseWriter.WriteHeader(http.StatusNoContent)
}

func doPOST(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		canonicalStorageUrl            string
		canonicalStorageUrlTransaction *canonicalStorageUrlTransactionStruct
		err                            error
		httpStatusCode                 int
		ok                             bool
	)

	swallowBody(request)

	canonicalStorageUrl = fetchCanonicalStorageUrl(request)
	if canonicalStorageUrl == "" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	lockCanonicalStorageUrl(canonicalStorageUrl)

	_, httpStatusCode, err = swiftObjectGet(request.Header.Get("X-Storage-Url"), request.Header.Get("X-Auth-Token"))
	if (err != nil) || (httpStatusCode < 200) || (httpStatusCode > 299) {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		if err != nil {
			httpStatusCode = http.StatusInternalServerError
		}
		responseWriter.WriteHeader(httpStatusCode)
		return
	}

	canonicalStorageUrlTransaction, ok = fetchCanonicalStorageUrlTransaction(canonicalStorageUrl)
	if !ok {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		responseWriter.WriteHeader(http.StatusGone)
		return
	}
	if canonicalStorageUrlTransaction.path != request.URL.Path {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		responseWriter.WriteHeader(http.StatusGone)
		return
	}

	if canonicalStorageUrlTransaction.isPut {
		err = os.Rename(globals.config.DataBasePath+"/"+canonicalStorageUrl+"_next", globals.config.DataBasePath+"/"+canonicalStorageUrl)
		if err != nil {
			panic(fmt.Errorf("os.Rename(globals.config.DataBasePath+\"/\"+canonicalStorageUrl+\"_next\", globals.config.DataBasePath+\"/\"+canonicalStorageUrl) failed: %v", err))
		}
	} else {
		err = os.Remove(globals.config.DataBasePath + "/" + canonicalStorageUrl)
		if err != nil {
			panic(fmt.Errorf("os.Remove(globals.config.DataBasePath+\"/\"+canonicalStorageUrl) failed: %v", err))
		}
	}

	canonicalStorageUrlTransaction.end()

	unlockCanonicalStorageUrl(canonicalStorageUrl)

	responseWriter.WriteHeader(http.StatusOK)
}

func doPUT(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		body                []byte
		canonicalStorageUrl string
		err                 error
		httpStatusCode      int
	)

	body = readBody(request)

	canonicalStorageUrl = fetchCanonicalStorageUrl(request)
	if canonicalStorageUrl == "" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	lockCanonicalStorageUrl(canonicalStorageUrl)

	_, httpStatusCode, err = swiftObjectGet(request.Header.Get("X-Storage-Url"), request.Header.Get("X-Auth-Token"))
	if (err != nil) || (httpStatusCode < 200) || (httpStatusCode > 299) {
		unlockCanonicalStorageUrl(canonicalStorageUrl)
		if err != nil {
			httpStatusCode = http.StatusInternalServerError
		}
		responseWriter.WriteHeader(httpStatusCode)
		return
	}

	beginCanonicalStorageUrlTransaction(canonicalStorageUrl, request.URL.Path, true)

	err = os.WriteFile(globals.config.DataBasePath+"/"+canonicalStorageUrl+"_next", body, 0666)
	if err != nil {
		panic(fmt.Errorf("os.WriteFile(globals.config.DataBasePath + \"/\" + canonicalStorageUrl + \"_next\", body, 0666) failed: %v", err))
	}

	unlockCanonicalStorageUrl(canonicalStorageUrl)

	responseWriter.WriteHeader(http.StatusNoContent)
}
