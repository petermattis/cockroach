// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)
//
// This file implements the visitors needed to extract aggregation expressions
// as needed for distsql physical planning of aggregators.
// See extractAggExprs and extractPostAggExprs.

package sql

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

type aggExprVisitor struct {
	exprs []distsqlrun.AggregatorSpec_Expr
}

var _ parser.Visitor = &aggExprVisitor{}

// Our base case is when our expression is of type *aggregateFuncHolder, in
// which case we construct the equivalent AggregatorSpec_Expr and accumulate it.
// If our expression is NOT of the type *aggregateFuncHolder we may have an
// expression like 'COUNT(k) + 1', we recurse.
func (v *aggExprVisitor) VisitPre(expr parser.Expr) (bool, parser.Expr) {
	fholder, ok := expr.(*aggregateFuncHolder)
	if !ok {
		return true, expr
	}

	f, ok := fholder.expr.(*parser.FuncExpr)
	if !ok {
		aggexpr := distsqlrun.AggregatorSpec_Expr{
			Func: distsqlrun.AggregatorSpec_IDENT,
		}
		v.exprs = append(v.exprs, aggexpr)
		return false, expr
	}

	aggexpr := distsqlrun.AggregatorSpec_Expr{
		Func: distsqlrun.AggregatorSpec_Func(
			distsqlrun.AggregatorSpec_Func_value[strings.ToUpper(
				f.Func.FunctionReference.String(),
			)],
		),
		Distinct: f.Type == parser.DistinctFuncType,
	}
	v.exprs = append(v.exprs, aggexpr)
	return false, expr
}

func (v *aggExprVisitor) VisitPost(expr parser.Expr) parser.Expr {
	return expr
}

func (v aggExprVisitor) extract(typedExpr parser.TypedExpr) []distsqlrun.AggregatorSpec_Expr {
	parser.WalkExprConst(&v, typedExpr)
	return v.exprs
}

// extractAggExprs translates the render expressions into the form needed by
// the AggregatorSpec where per "aggregation function" we specify the function
// type (SUM, COUNT, IDENT, etc) and the column this function is going to be
// operating on (@1, @2, @3, etc).
// In order to do this we need to "flatten" out the render expressions.
// The following examples detail out what we mean by this.
//
// - 'SELECT COUNT(k), SUM(v + w), v + w FROM kv GROUP BY v + w'
//   The output schema of the selectNode (groupNode's source) here is
//   [k v+w v+w v+w], the render expressions we are concerned with are
//   [k v+w v+w].
//   We see for COUNT, the corresponding input stream is the very first (k).
//   For SUM it is the second (v+w) and for 'v + w' we use the IDENT aggregation
//   function.
//   Concisely: for the n-th aggregation function we operate on the n-th stream.
//
// - 'SELECT COUNT(k) + COUNT(v) FROM kv'
//   The output schema of groupNode's source here is [k v], the render
//   expressions we are concerned with are [k v]. We see that we need a COUNT
//   to operate on the first stream, another COUNT to operate on the second.
//   Again we see that for the n-th aggregation function we operate on the n-th
//   stream (note, this is distinct from the n-th render expression).
//
//   NB: The actual addition in 'COUNT(k) + COUNT(v)' will be computed in
//   postAggExprVisitor.
func (dsp *distSQLPlanner) extractAggExprs(
	render []parser.TypedExpr,
) (aggExprs []distsqlrun.AggregatorSpec_Expr) {
	v := aggExprVisitor{}
	for _, expr := range render {
		aggExprs = append(aggExprs, v.extract(expr)...)
	}
	for i := range aggExprs {
		aggExprs[i].ColIdx = uint32(i)
	}
	return aggExprs
}

type postAggExprVisitor struct {
	count int
}

var _ parser.Visitor = &postAggExprVisitor{}

func (v *postAggExprVisitor) VisitPre(expr parser.Expr) (bool, parser.Expr) {
	return true, expr
}

// Our base case is the *aggregateFuncHolder, every time we come across one we
// substitute it with an ordinal reference. An expression of any other type is
// left as is.
//
// We make the deliberate choice of doing this transformation in VisitPost
// instead of VisitPre, an example to demonstrate why is that if we have
// 'SELECT COUNT(k) + COUNT(v) FROM kv', for our render expression
// 'COUNT(k) + COUNT(v)' we want it to be substituted by @1 + @2. Therefore we
// need to first substitute the "leaf nodes" of this expression (COUNT(k),
// COUNT(v)) before evaluating the binary "+" expression.
func (v *postAggExprVisitor) VisitPost(expr parser.Expr) parser.Expr {
	if _, ok := expr.(*aggregateFuncHolder); !ok {
		return expr
	}

	newExpr := parser.NewOrdinalReference(v.count)
	v.count++
	return newExpr
}

func (v *postAggExprVisitor) extract(typedExpr parser.TypedExpr) parser.TypedExpr {
	expr, _ := parser.WalkExpr(v, typedExpr)
	return expr.(parser.TypedExpr)
}

// extractPostAggrExprs returns a list of expressions to be evaluated following
// the aggregation stage.
// For e.g. if we have 'SELECT COUNT(k)+COUNT(v) FROM kv', the output schema of
// our aggregator would be [count(k) count(v)], but the output schema of the
// current groupNode implementation is [count(k)+count(v)], to bridge this gap
// we need to introduce an aggregator with the expression [@1 + @2].
// There's one subtle thing to note here:
// - We need to account for the "flattening" of the render expressions that had
//   to take place earlier.
//
// The following example details out the considerations we had to make.
// - 'SELECT COUNT(k), COUNT(v), COUNT(k) + COUNT(v) FROM kv'
// The output schema of our aggregator is [count(k) count(v) count(k) count(v)].
// The post aggregation evaluator we need to add has to be instantiated with the
// expression [@1 @2 @3 + @4].
// Note that, as before, the n-th render expression is distinct from the n-th
// stream. Therefore we need to maintain this "counter" state as we iterate
// through each of the render expressions, assigning a monotonically increasing
// ordinal reference to each instance of an *aggregateFuncHolder (our base
// case).
// To this end we have a pointer receiver for postAggExprVisitor.extract(...).
//
// Additionally if we see that all of our evalExprs are simply of type
// *parser.IndexedVar, we have an evaluator that simply forwards every single
// row without any transformation/evaluation. In this case we return false for
// needEval.
func (dsp *distSQLPlanner) extractPostAggrExprs(
	render []parser.TypedExpr,
) (evalExprs []parser.TypedExpr, needEval bool) {
	v := postAggExprVisitor{}
	for _, expr := range render {
		result := v.extract(expr)
		if _, ok := result.(*parser.IndexedVar); !ok {
			needEval = true
		}
		evalExprs = append(evalExprs, result)
	}
	return evalExprs, needEval
}
