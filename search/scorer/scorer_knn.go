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

package scorer

import (
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeKNNQueryScorer int

func init() {
	var sqs KNNQueryScorer
	reflectStaticSizeKNNQueryScorer = int(reflect.TypeOf(sqs).Size())
}

type KNNQueryScorer struct {
	queryVector            []float32
	queryField             string
	queryWeight            float64
	queryBoost             float64
	queryNorm              float64
	docTerm                uint64
	docTotal               uint64
	options                search.SearcherOptions
	includeScore           bool
	similarityMetric       string
	queryWeightExplanation *search.Explanation
}

func NewKNNQueryScorer(queryVector []float32, queryField string, queryBoost float64,
	docTerm uint64, docTotal uint64, options search.SearcherOptions,
	similarityMetric string) *KNNQueryScorer {
	return &KNNQueryScorer{
		queryVector:      queryVector,
		queryField:       queryField,
		queryBoost:       queryBoost,
		queryWeight:      1.0,
		docTerm:          docTerm,
		docTotal:         docTotal,
		options:          options,
		includeScore:     options.Score != "none",
		similarityMetric: similarityMetric,
	}
}

func (sqs *KNNQueryScorer) Score(ctx *search.SearchContext,
	knnMatch *index.VectorDoc) *search.DocumentMatch {
	rv := ctx.DocumentMatchPool.Get()

	if sqs.includeScore || sqs.options.Explain {
		var scoreExplanation *search.Explanation
		score := knnMatch.Score
		var similarityScoreExpl *search.Explanation
		switch sqs.similarityMetric {
		case util.EuclideanDistance:
			// eucliden distances need to be inverted so as to keep consistent
			// with the usual document relevancy ordering as per the score.
			score = 1.0 / score
			fallthrough
		default:
			if sqs.options.Explain {
				similarityScoreExpl = &search.Explanation{
					Value:   score,
					Message: fmt.Sprintf("similarityScore(%s:%v)=%f", sqs.queryField, sqs.queryVector, score),
				}
			}
		}

		if sqs.options.Explain {
			childrenExplanations := make([]*search.Explanation, 1)
			childrenExplanations[0] = similarityScoreExpl
			scoreExplanation = &search.Explanation{
				Value:    score,
				Message:  fmt.Sprintf("fieldWeight(%s:%v in %s), product of:", sqs.queryField, sqs.queryVector, knnMatch.ID),
				Children: childrenExplanations,
			}
		}

		// if the query weight isn't 1, multiply
		if sqs.queryWeight != 1.0 {
			score = score * sqs.queryWeight
			if sqs.options.Explain {
				childExplanations := make([]*search.Explanation, 2)
				childExplanations[0] = sqs.queryWeightExplanation
				childExplanations[1] = scoreExplanation
				scoreExplanation = &search.Explanation{
					Value:    score,
					Message:  fmt.Sprintf("weight(%s:%v^%f in %s), product of:", sqs.queryField, sqs.queryVector, sqs.queryBoost, knnMatch.ID),
					Children: childExplanations,
				}
			}
		}

		if sqs.includeScore {
			rv.Score = score
		}

		if sqs.options.Explain {
			rv.Expl = scoreExplanation
		}
	}

	rv.IndexInternalID = append(rv.IndexInternalID, knnMatch.ID...)
	return rv
}

func (sqs *KNNQueryScorer) Weight() float64 {
	return sqs.queryBoost * sqs.queryBoost
}

func (sqs *KNNQueryScorer) SetQueryNorm(qnorm float64) {
	sqs.queryNorm = qnorm

	// update the query weight
	sqs.queryWeight = sqs.queryBoost * sqs.queryNorm
	if sqs.options.Explain {
		childrenExplanations := make([]*search.Explanation, 2)
		childrenExplanations[0] = &search.Explanation{
			Value:   sqs.queryBoost,
			Message: "boost",
		}
		childrenExplanations[1] = &search.Explanation{
			Value:   sqs.queryNorm,
			Message: "queryNorm",
		}
		sqs.queryWeightExplanation = &search.Explanation{
			Value:    sqs.queryWeight,
			Message:  fmt.Sprintf("queryWeight(%s:%v^%f), product of:", sqs.queryField, sqs.queryVector, sqs.queryBoost),
			Children: childrenExplanations,
		}
	}
}
