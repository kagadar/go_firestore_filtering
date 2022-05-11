// Copyright 2022 The Go Firestore Filtering Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filterstore

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/iancoleman/strcase"
	"github.com/kagadar/go_proto_expression/protoexpr"
	"go.einride.tech/aip/filtering"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type transpiler[T proto.Message] struct {
	client *firestore.Client
}

func (t transpiler[T]) Transpile(ctx context.Context, factory func() T, parent, collection, pageToken string, pageSize int32, filter filtering.Filter) ([]T, string, error) {
	q := &query{q: t.client.Collection(fmt.Sprintf("%s/%s", parent, collection)).Limit(int(pageSize)), types: filter.CheckedExpr.GetTypeMap()}
	if err := q.transpile(filter.CheckedExpr.GetExpr(), false); err != nil {
		return nil, "", err
	}
	if pageToken != "" {
		q.q = q.q.OrderBy(firestore.DocumentID, firestore.Asc)
		q.startAfter = append(q.startAfter, pageToken)
	}
	if len(q.startAfter) > 0 {
		q.q = q.q.StartAfter(q.startAfter...)
	}
	docs, err := q.q.Documents(ctx).GetAll()
	if err != nil {
		return nil, "", err
	}
	data := make([]T, len(docs))
	for i, doc := range docs {
		data[i] = factory()
		doc.DataTo(data[i])
	}
	return data, "", nil
}

// Creates a new Firestore transpiler for requests to the specified List method.
func New[T proto.Message](client *firestore.Client, mtd protoreflect.MethodDescriptor, msg T) (protoexpr.Transpiler[T], error) {
	return protoexpr.New[T](transpiler[T]{client: client}, mtd, msg)
}

// Returns the appropriate firestore operator for the specified function.
func operator(function string, not bool) (string, error) {
	switch function {
	case filtering.FunctionEquals:
		if not {
			return "!=", nil
		}
		return "==", nil
	case filtering.FunctionNotEquals:
		if not {
			return "==", nil
		}
		return "!=", nil
	case filtering.FunctionLessThan:
		if not {
			return ">=", nil
		}
		return "<", nil
	case filtering.FunctionLessEquals:
		if not {
			return ">", nil
		}
		return "<=", nil
	case filtering.FunctionGreaterThan:
		if not {
			return "<=", nil
		}
		return ">", nil
	case filtering.FunctionGreaterEquals:
		if not {
			return "<", nil
		}
		return ">=", nil
	}
	notStr := ""
	if not {
		notStr = "NOT "
	}
	return "", status.Errorf(codes.InvalidArgument, "no Firestore operator for %s%s", notStr, function)
}

// Returns the Firestore path for the provided Expr.
func toPath(e *expr.Expr) (string, error) {
	switch e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		p, err := toPath(e.GetSelectExpr().GetOperand())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s.%s", p, strcase.ToCamel(e.GetSelectExpr().GetField())), nil
	case *expr.Expr_IdentExpr:
		return strcase.ToCamel(e.GetIdentExpr().GetName()), nil
	}
	return "", status.Errorf(codes.InvalidArgument, "unable to get path for expression: %v", e)
}

func unwrapConst(c *expr.Constant) interface{} {
	switch c.ConstantKind.(type) {
	case *expr.Constant_BoolValue:
		return c.GetBoolValue()
	case *expr.Constant_BytesValue:
		return c.GetBytesValue()
	case *expr.Constant_DoubleValue:
		return c.GetDoubleValue()
	case *expr.Constant_Int64Value:
		return c.GetInt64Value()
	case *expr.Constant_StringValue:
		return c.GetStringValue()
	case *expr.Constant_Uint64Value:
		return c.GetUint64Value()
	default:
		return nil
	}
}

type query struct {
	q          firestore.Query
	subqueries []*query
	types      map[int64]*expr.Type
	// Firestore only allows one field to participate in inequality:
	// https://firebase.google.com/docs/firestore/query-data/queries#query_limitations
	// If an inequality call is made on more than one field, reject the filter.
	inequality string
	startAfter []interface{}
}

// Checks if an inequality has already been set in this query.
// If set to a path other than the one provided, the query is invalid.
func (q *query) setInequality(path string) error {
	if q.inequality == "" {
		q.inequality = path
	} else if q.inequality != path {
		return status.Error(codes.InvalidArgument, "inequality can only be used on a single field")
	}
	return nil
}

// Checks if the specified field has a value.
func (q *query) transpileHas(e *expr.Expr_Call, not bool) error {
	if len(e.Args) != 2 {
		return status.Error(codes.InvalidArgument, ": requires two arguments")
	}
	switch q.types[e.Args[0].Id].GetTypeKind().(type) {
	case *expr.Type_MessageType:
		path, err := toPath(e.Args[0])
		if err != nil {
			return err
		}
		path = fmt.Sprintf("%s.%s", path, strcase.ToCamel(e.Args[1].GetConstExpr().GetStringValue()))
		path = path[strings.Index(path, ".")+1:]
		if not {
			q.q = q.q.Where(path, "==", nil)
			return nil
		}
		if err := q.setInequality(path); err != nil {
			return err
		}
		q.startAfter = append(q.startAfter, nil)
		q.q = q.q.OrderBy(path, firestore.Asc)
		return nil
	case *expr.Type_ListType_:
		// TODO(kagadar): Use `array-contains`
	case *expr.Type_MapType_:
		// TODO(kagadar): map differs from message maybe?
	}
	return status.Error(codes.InvalidArgument, ": must be used on a message, map or list")
}

func (q *query) transpileEquality(e *expr.Expr_Call, not bool) error {
	if len(e.Args) != 2 {
		return status.Errorf(codes.InvalidArgument, "%s requires two arguments", e.Function)
	}
	op, err := operator(e.Function, not)
	if err != nil {
		return err
	}
	path, err := toPath(e.Args[0])
	if err != nil {
		return err
	}
	if op != "==" {
		if err := q.setInequality(path); err != nil {
			return err
		}
	}
	q.q = q.q.Where(path, op, unwrapConst(e.Args[1].GetConstExpr()))
	return nil
}

func (q *query) transpileCall(e *expr.Expr_Call, not bool) error {
	if e.Function == filtering.FunctionNot {
		if len(e.Args) != 1 {
			return status.Error(codes.InvalidArgument, "NOT requires one argument")
		}
		return q.transpile(e.Args[0], !not)
	}
	switch e.Function {
	case filtering.FunctionHas:
		return q.transpileHas(e, not)
	case filtering.FunctionEquals, filtering.FunctionNotEquals,
		filtering.FunctionLessThan, filtering.FunctionLessEquals,
		filtering.FunctionGreaterThan, filtering.FunctionGreaterEquals:
		return q.transpileEquality(e, not)
	case filtering.FunctionAnd:
		if len(e.Args) != 2 {
			return status.Error(codes.InvalidArgument, "AND requires two arguments")
		}
		if err := q.transpile(e.Args[0], not); err != nil {
			return err
		}
		return q.transpile(e.Args[1], not)
	case filtering.FunctionOr:
		// TODO(kagadar): Split into two queries
	}
	return status.Errorf(codes.InvalidArgument, "unknown filter function %s", e.Function)
}

func (q *query) transpile(e *expr.Expr, not bool) error {
	if e == nil {
		return nil
	}
	switch e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		return q.transpileCall(e.GetCallExpr(), not)
	case *expr.Expr_ConstExpr:
		// TODO(kagadar): search all searchable fields (FUZZY)
	default:
		// Unclear if other expressions can exist here.
		log.Printf("unexpected expression: %v", e)
	}
	return status.Error(codes.InvalidArgument, "invalid filter expression")
}
