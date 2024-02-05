//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build vectors
// +build vectors

package scorch

import (
	"context"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

type OptimizeVR struct {
	ctx       context.Context
	snapshot  *IndexSnapshot
	totalCost uint64
	// maps field to vector readers
	vrs map[string][]*IndexSnapshotVectorReader
}

// This setting _MUST_ only be changed during init and not after.
var BleveMaxKNNConcurrency = 10

func (o *OptimizeVR) Finish() error {
	// for each field, get the vector index --> invoke the zap func.
	// for each VR, populate postings list and iterators
	// by passing the obtained vector index and getting similar vectors.
	// defer close index - just once.
	var errorsM sync.Mutex
	var errors []error
	if cb := o.ctx.Value(search.SearchSearcherEndCallbackKey); cb != nil {
		if cbF, ok := cb.(search.SearchSearcherEndCallbackFn); ok {
			defer func() {
				// notify the callback that the searcher creation etc. is finished
				// and report back the total cost for it to track and take actions
				// appropriately.
				_ = cbF(o.totalCost)
			}()
		}
	}

	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, BleveMaxKNNConcurrency)
	// Launch goroutines to get vector index for each segment
	for i, seg := range o.snapshot.segment {
		if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire a semaphore slot
			go func(index int, segment segment_api.VectorSegment, origSeg *SegmentSnapshot) {
				defer func() {
					<-semaphore // Release the semaphore slot
					wg.Done()
				}()
				for field, vrs := range o.vrs {
					vectorIndexInterpret, err := segment.InterpretVectorIndex(field)
					if err != nil {
						errorsM.Lock()
						errors = append(errors, err)
						errorsM.Unlock()
						return
					}

					// update the vector index size as a meta value in the segment snapshot
					vectorIndexSize := vectorIndexInterpret.Size()
					seg.cachedMeta.updateMeta(field, vectorIndexSize)
					for _, vr := range vrs {
						// for each VR, populate postings list and iterators
						// by passing the obtained vector index and getting similar vectors.
						pl, err := vectorIndexInterpret.Search(vr.vector, vr.k, origSeg.deleted)
						if err != nil {
							errorsM.Lock()
							errors = append(errors, err)
							errorsM.Unlock()
							go vectorIndexInterpret.Close()
							return
						}

						// postings and iterators are already alloc'ed when
						// IndexSnapshotVectorReader is created
						vr.postings[index] = pl
						vr.iterators[index] = pl.Iterator(vr.iterators[index])
					}
					go vectorIndexInterpret.Close()
				}
			}(i, sv, seg)
		}
	}
	wg.Wait()
	close(semaphore)
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (s *IndexSnapshotVectorReader) VectorOptimize(ctx context.Context,
	octx index.VectorOptimizableContext) (index.VectorOptimizableContext, error) {

	if s.snapshot.parent.segPlugin.Version() < VectorSearchSupportedSegmentVersion {
		return nil, fmt.Errorf("vector search not supported for this index, "+
			"index's segment version %v, supported segment version for vector search %v",
			s.snapshot.parent.segPlugin.Version(), VectorSearchSupportedSegmentVersion)
	}

	if octx == nil {
		octx = &OptimizeVR{snapshot: s.snapshot,
			vrs: make(map[string][]*IndexSnapshotVectorReader),
		}
	}

	o, ok := octx.(*OptimizeVR)
	if !ok {
		return octx, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize KNN across different snapshots")
	}

	// for every searcher creation, consult the segment snapshot to see
	// what's the vector index size and since you're anyways going
	// to use this vector index to perform the search etc. as part of the Finish()
	// perform a check as to whether we allow the searcher creation (the downstream)
	// Finish() logic to even occur or not.
	var sumVectorIndexSize uint64
	for _, seg := range o.snapshot.segment {
		vecIndexSize := seg.cachedMeta.fetchMeta(s.field)
		if vecIndexSize != nil {
			sumVectorIndexSize += vecIndexSize.(uint64)
		}
	}

	if cb := o.ctx.Value(search.SearchSearcherStartCallbackKey); cb != nil {
		if cbF, ok := cb.(search.SearchSearcherStartCallbackFn); ok {
			err := cbF(sumVectorIndexSize)
			if err != nil {
				return nil, err
			}
		}
	}

	// total cost is essentially the sum of the vector indexes' size across all the
	// searchers - all of them end up reading and maintaining a vector index.
	// misacconting this value would end up calling the "end" callback with a value
	// not equal to the value passed to "start" callback.
	o.totalCost += sumVectorIndexSize

	o.vrs[s.field] = append(o.vrs[s.field], s)
	o.ctx = ctx
	return o, nil
}
