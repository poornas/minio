/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

// creates a temp dir and sets up posix layer.
// returns posix layer, temp dir path to be used for the purpose of tests.
func newCacheTestSetup(t *testing.T) (string, error) {
	start := UTCNow()

	diskPath, err := ioutil.TempDir("/tmp", "minio-")
	if err != nil {
		return "", err
	}
	//num := 10000000
	num := 10
	for i := 0; i < num; i++ {
		dirName := fmt.Sprintf("dir%d", i)
		if err := os.MkdirAll(pathJoin(diskPath, dirName), 0777); err != nil {
			return "", err
		}
		if _, err := os.Create(pathJoin(diskPath, dirName, "part.1")); err != nil {
			return "", err
		}
	}
	elapsed := time.Since(start)
	t.Logf("created %d entries in %s", num, elapsed)

	return diskPath, nil
}
func timeTrack(t *testing.T, start time.Time, name string) {

}

func TestPosixListSpeed(t *testing.T) {
	dirName, err := newCacheTestSetup(t)
	if err != nil {
		t.Log("failed to create cache test setup ::", err)
	}

	start := UTCNow()

	entries, err := listCacheDir(dirName)
	elapsed := time.Since(start)

	t.Logf("listed %d entries in %s", len(entries), elapsed)
	start = UTCNow()
	sort.Sort(ByAtime(entries))
	timeElapsed := time.Since(start)
	t.Logf("sorted %d entries in %s  ", len(entries), timeElapsed)

}

func TestGetCacheControlOpts(t *testing.T) {
	expiry, _ := time.Parse(http.TimeFormat, "Wed, 21 Oct 2015 07:28:00 GMT")

	testCases := []struct {
		cacheControlHeaderVal string
		expiryHeaderVal       time.Time
		expectedCacheControl  cacheControl
		expectedErr           bool
	}{
		{"", timeSentinel, cacheControl{}, false},
		{"max-age=2592000, public", timeSentinel, cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"max-age=2592000, no-store", timeSentinel, cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"must-revalidate, max-age=600", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", expiry, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Date(2015, time.October, 21, 07, 28, 00, 00, time.UTC)}, false},
		{"s-maxAge=2500, max-age=600s", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, true},
	}
	var m map[string]string

	for i, testCase := range testCases {
		m = make(map[string]string)
		m["cache-control"] = testCase.cacheControlHeaderVal
		if testCase.expiryHeaderVal != timeSentinel {
			m["expires"] = testCase.expiryHeaderVal.String()
		}
		c := cacheControlOpts(ObjectInfo{UserDefined: m, Expires: testCase.expiryHeaderVal})
		if testCase.expectedErr && (c != cacheControl{}) {
			t.Errorf("expected err for case %d", i)
		}
		if !testCase.expectedErr && !reflect.DeepEqual(c, testCase.expectedCacheControl) {
			t.Errorf("expected  %v got %v for case %d", testCase.expectedCacheControl, c, i)
		}

	}
}
