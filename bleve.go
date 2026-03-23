package server

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"

	storage "github.com/slidebolt/sb-storage-sdk"
)

// buildBleveQuery translates storage filters into a Bleve conjunction query.
func buildBleveQuery(filters []storage.Filter) (query.Query, error) {
	var queries []query.Query
	for _, f := range filters {
		q, err := filterToBleve(f)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}
	if len(queries) == 1 {
		return queries[0], nil
	}
	return bleve.NewConjunctionQuery(queries...), nil
}

func filterToBleve(f storage.Filter) (query.Query, error) {
	switch f.Op {
	case storage.Eq:
		return termQuery(f.Field, f.Value), nil
	case storage.Neq:
		return negateQuery(termQuery(f.Field, f.Value)), nil
	case storage.Gt:
		return numericRange(f.Field, f.Value, true, nil, false)
	case storage.Gte:
		return numericRange(f.Field, f.Value, false, nil, false)
	case storage.Lt:
		return numericRange(f.Field, nil, false, f.Value, true)
	case storage.Lte:
		return numericRange(f.Field, nil, false, f.Value, false)
	case storage.Contains:
		s, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf("contains: value must be string")
		}
		q := bleve.NewWildcardQuery("*" + s + "*")
		q.SetField(f.Field)
		return q, nil
	case storage.Prefix:
		s, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf("prefix: value must be string")
		}
		q := bleve.NewPrefixQuery(s)
		q.SetField(f.Field)
		return q, nil
	case storage.In:
		s, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf("in: value must be string")
		}
		q := bleve.NewMatchQuery(s)
		q.SetField(f.Field)
		return q, nil
	case storage.Exists:
		// Match any document that has this field with any value.
		q := bleve.NewWildcardQuery("*")
		q.SetField(f.Field)
		return q, nil
	default:
		return nil, fmt.Errorf("unknown operator: %s", f.Op)
	}
}

func termQuery(field string, value any) query.Query {
	switch v := value.(type) {
	case bool:
		q := bleve.NewBoolFieldQuery(v)
		q.SetField(field)
		return q
	case float64:
		inclusive := true
		q := bleve.NewNumericRangeInclusiveQuery(&v, &v, &inclusive, &inclusive)
		q.SetField(field)
		return q
	default:
		s := fmt.Sprintf("%v", v)
		q := bleve.NewMatchQuery(s)
		q.SetField(field)
		return q
	}
}

func negateQuery(inner query.Query) query.Query {
	matchAll := bleve.NewMatchAllQuery()
	boolQ := bleve.NewBooleanQuery()
	boolQ.AddMust(matchAll)
	boolQ.AddMustNot(inner)
	return boolQ
}

func numericRange(field string, min any, minExclusive bool, max any, maxExclusive bool) (query.Query, error) {
	var minF, maxF *float64
	minInc := true
	maxInc := true

	if min != nil {
		f, ok := toFloat64(min)
		if !ok {
			return nil, fmt.Errorf("numeric filter: cannot convert %v to number", min)
		}
		if minExclusive {
			// Bleve uses inclusive boundaries, so shift slightly.
			// Instead, we use inclusive=false via the inclusive param.
			minInc = false
		}
		minF = &f
	}
	if max != nil {
		f, ok := toFloat64(max)
		if !ok {
			return nil, fmt.Errorf("numeric filter: cannot convert %v to number", max)
		}
		if maxExclusive {
			maxInc = false
		}
		maxF = &f
	}

	q := bleve.NewNumericRangeInclusiveQuery(minF, maxF, &minInc, &maxInc)
	q.SetField(field)
	return q, nil
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
