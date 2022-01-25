// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

func logFatal(err error) {
	logf("FATAL", "%v", err)
	logStack()
	os.Exit(1)
}

func logFatalf(format string, args ...interface{}) {
	logf("FATAL", format, args...)
	logStack()
	os.Exit(1)
}

func logError(err error) {
	logf("ERROR", "%v", err)
}

func logErrorf(format string, args ...interface{}) {
	logf("ERROR", format, args...)
}

func logWarn(err error) {
	logf("WARN", "%v", err)
}

func logWarnf(format string, args ...interface{}) {
	logf("WARN", format, args...)
}

func logInfo(err error) {
	logf("INFO", "%v", err)
}

func logInfof(format string, args ...interface{}) {
	logf("INFO", format, args...)
}

func logTrace(err error) {
	if globals.config.TraceEnabled {
		logf("TRACE", "%v", err)
	}
}

func logTracef(format string, args ...interface{}) {
	if globals.config.TraceEnabled {
		logf("TRACE", format, args...)
	}
}

func logStack() {
	const (
		runtimeStackBufSize = 65536
	)

	var (
		runtimeStackBuf     []byte
		runtimeStackBufUsed int
		runtimeStackString  string
	)

	runtimeStackBuf = make([]byte, runtimeStackBufSize)
	runtimeStackBufUsed = runtime.Stack(runtimeStackBuf, true)
	runtimeStackString = string(runtimeStackBuf[:runtimeStackBufUsed])

	logf("STACK", "\n%s", runtimeStackString)
}

func logf(level string, format string, args ...interface{}) {
	var (
		enhancedArgs   []interface{}
		enhancedFormat string
		err            error
		logMsg         string
	)

	enhancedFormat = "[%s][%s] " + format
	enhancedArgs = append([]interface{}{time.Now().Format(time.RFC3339Nano), level}, args...)

	logMsg = fmt.Sprintf(enhancedFormat, enhancedArgs[:]...)

	if globals.logFile == nil {
		if globals.config.LogFilePath != "" {
			globals.logFile, err = os.OpenFile(globals.config.LogFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
			if err == nil {
				_, _ = globals.logFile.WriteString(logMsg + "\n")
			} else {
				globals.logFile = nil
			}
		}
	} else {
		globals.logFile.WriteString(logMsg + "\n")
	}
	if globals.config.LogToConsole {
		fmt.Println(logMsg)
	}
}

func logSIGHUP() {
	if globals.logFile != nil {
		_ = globals.logFile.Close()
		globals.logFile = nil
	}
}

func newLogger() *log.Logger {
	return log.New(&globals, "", 0)
}

func (dummy *globalsStruct) Write(p []byte) (n int, err error) {
	if globals.config.FUSELogEnabled {
		logf("FISSION", "%s", string(p[:]))
	}
	return 0, nil
}
