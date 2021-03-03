/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file $GOFILE -unexported
//msgp:ignore tierJournal walkfn

type tierJournal struct {
	sync.RWMutex
	diskPath string
	file     *os.File // active journal file
}

type jentry struct {
	ObjName  string `msg:"obj"`
	TierName string `msg:"tier"`
}

const (
	tierJournalVersion = 1
	tierJournalHdrLen  = 2 // 2 bytes
)

func initTierDeletionJournal(done <-chan struct{}) (*tierJournal, error) {
	diskPath, ok := globalEndpoints.FirstLocalDiskPath()
	if !ok {
		return nil, errors.New("failed to find first local disk path")
	}

	j := &tierJournal{
		diskPath: diskPath,
	}

	if err := os.MkdirAll(filepath.Dir(j.JournalPath()), os.FileMode(0770)); err != nil {
		return nil, err
	}

	err := j.Open()
	if err != nil {
		return nil, err
	}

	go j.deletePending(done)
	return j, nil
}

// rotate rotates the journal. If a read-only journal already exists it does
// nothing. Otherwise renames the active journal to a read-only journal and
// opens a new active journal.
func (j *tierJournal) rotate() error {
	// Do nothing if a read-only journal file already exists.
	if _, err := os.Stat(j.ReadOnlyPath()); err == nil {
		return nil
	}
	// Close the active journal if present.
	j.Close()
	// Open a new active journal for subsequent journalling.
	return j.Open()
}

type walkFn func(objName, tierName string) error

func (j *tierJournal) ReadOnlyPath() string {
	return filepath.Join(j.diskPath, minioMetaBucket, "ilm", "deletion-journal.ro.bin")
}

func (j *tierJournal) JournalPath() string {
	return filepath.Join(j.diskPath, minioMetaBucket, "ilm", "deletion-journal.bin")
}

func (j *tierJournal) WalkEntries(fn walkFn) {
	err := j.rotate()
	if err != nil {
		logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to rotate pending deletes journal %s", err))
		return
	}

	ro, err := j.OpenRO()
	switch {
	case errors.Is(err, os.ErrNotExist):
		return // No read-only journal to process; nothing to do.
	case err != nil:
		logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed open read-only journal for processing %s", err))
		return
	}
	defer ro.Close()
	mr := msgp.NewReader(ro)
	done := false
	for {
		var entry jentry
		err := entry.DecodeMsg(mr)
		if errors.Is(err, io.EOF) {
			done = true
			break
		}
		if err != nil {
			logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to decode journal entry %s", err))
			break
		}
		err = fn(entry.ObjName, entry.TierName)
		if err != nil && !isErrObjectNotFound(err) {
			logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to delete transitioned object %s from %s due to %s", entry.ObjName, entry.TierName, err))
			j.AddEntry(entry)
		}
	}
	if done {
		os.Remove(j.ReadOnlyPath())
	}
}

func deleteObjectFromRemoteTier(objName, tierName string) error {
	w, err := globalTierConfigMgr.getDriver(tierName)
	if err != nil {
		return err
	}
	err = w.Remove(context.Background(), objName)
	if err != nil {
		return err
	}
	return nil
}

func (j *tierJournal) deletePending(done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			j.WalkEntries(deleteObjectFromRemoteTier)

		case <-done:
			j.Close()
			return
		}
	}
}

func (j *tierJournal) AddEntry(je jentry) error {
	// Open journal if it hasn't been
	err := j.Open()
	if err != nil {
		return err
	}

	b, err := je.MarshalMsg(nil)
	if err != nil {
		return err
	}

	j.Lock()
	defer j.Unlock()
	_, err = j.file.Write(b)
	if err != nil {
		j.file = nil // reset to allow subsequent reopen when file/disk is available.
	}
	return err
}

// Close closes the active journal and renames it to read-only for pending
// deletes processing. Note: calling Close on a closed journal is a no-op.
func (j *tierJournal) Close() error {
	j.Lock()
	defer j.Unlock()
	if j.file == nil { // already closed
		return nil
	}

	var (
		f   *os.File
		fi  os.FileInfo
		err error
	)
	// Setting j.file to nil
	f, j.file = j.file, f
	if fi, err = f.Stat(); err != nil {
		return err
	}
	defer f.Close()
	// Skip renaming active journal if empty.
	if fi.Size() == tierJournalHdrLen {
		return nil
	}

	jPath := j.JournalPath()
	jroPath := j.ReadOnlyPath()
	// Rotate active journal to perform pending deletes.
	err = os.Rename(jPath, jroPath)
	if err != nil {
		return err
	}

	return nil
}

// Open opens a new active journal. Note: calling Open on an opened journal is a
// no-op.
func (j *tierJournal) Open() error {
	j.Lock()
	defer j.Unlock()
	if j.file != nil { // already open
		return nil
	}

	var err error
	j.file, err = os.OpenFile(j.JournalPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_DSYNC, 0644)
	if err != nil {
		return err
	}

	// write journal version header if active journal is empty
	fi, err := j.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		var data [tierJournalHdrLen]byte
		binary.LittleEndian.PutUint16(data[:], tierJournalVersion)
		_, err = j.file.Write(data[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *tierJournal) OpenRO() (io.ReadCloser, error) {
	file, err := os.Open(j.ReadOnlyPath())
	if err != nil {
		return nil, err
	}

	// read journal version header
	var data [tierJournalHdrLen]byte
	n, err := file.Read(data[:])
	if err != nil {
		return nil, err
	}
	if n < 2 {
		return nil, errors.New("invalid pending deletes journal format")
	}

	switch binary.LittleEndian.Uint16(data[:]) {
	case tierJournalVersion:
	default:
		return nil, errors.New("unsupported pending deletes journal version")
	}
	return file, nil
}
