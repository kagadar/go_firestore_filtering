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
	"go.einride.tech/aip/filtering"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/kagadar/go_proto_expression/protoexpr"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	opb "github.com/kagadar/go_proto_expression/genproto/options"
)

// AIP-132 & AIP-160 compliant List Request.
type ListRequest interface {
	GetParent() string
	GetPageSize() int32
	GetPageToken() string
	GetFilter() string
}

// AIP-132 compliant List Request.
type ListResponse interface {
	GetNextPageToken() string
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

type Transpiler interface {
	Transpile(context.Context, ListRequest) ([]*firestore.DocumentSnapshot, error)
}

type transpiler struct {
	client          *firestore.Client
	collection      string
	decls           *filtering.Declarations
	defaultPageSize int32
	maxPageSize     int32
}

// Creates a new transpiler for requests to the specified List method.
func New(client *firestore.Client, mtd protoreflect.MethodDescriptor) (Transpiler, error) {
	fs := transpiler{
		client:          client,
		defaultPageSize: 10,
		maxPageSize:     100,
	}
	if proto.HasExtension(mtd.Options(), opb.E_Pagination) {
		options := proto.GetExtension(mtd.Options(), opb.E_Pagination).(*opb.MethodPaginationOptions)
		if options.DefaultPageSize != nil {
			fs.defaultPageSize = options.GetDefaultPageSize()
		}
		if options.MaxPageSize != nil {
			fs.maxPageSize = options.GetMaxPageSize()
		}
	}
	var collectionFieldNum int32 = 1
	if proto.HasExtension(mtd.Output().Options(), opb.E_Collection) {
		options := proto.GetExtension(mtd.Output().Options(), opb.E_Collection).(*opb.MessageCollectionOptions)
		if options.CollectionFieldNumber != nil {
			collectionFieldNum = options.GetCollectionFieldNumber()
		}
	}
	collectionField := mtd.Output().Fields().ByNumber(protowire.Number(collectionFieldNum))
	if collectionField == nil || !collectionField.IsList() {
		return nil, status.Errorf(codes.InvalidArgument, "unable to determine collection field for %s", mtd.Output().FullName())
	}
	fs.collection = string(collectionField.Name())
	decls, err := filtering.NewDeclarations(append(append([]filtering.DeclarationOption{}, filtering.DeclareStandardFunctions()), protoexpr.Declare(collectionField.Message())...)...)
	if err != nil {
		return nil, err
	}
	fs.decls = decls
	return fs, nil
}

func operation(function string, not bool) (string, error) {
	switch function {
	case filtering.FunctionEquals:
		if not {
			return "==", nil
		}
		return "!=", nil
	case filtering.FunctionNotEquals:
	}
	notStr := ""
	if not {
		notStr = "NOT "
	}
	return "", status.Errorf(codes.InvalidArgument, "no Firestore operator for %s%s", notStr, function)
}

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
		if q.inequality == "" {
			q.inequality = path
		} else if q.inequality != path {
			return status.Error(codes.InvalidArgument, "inequality can only be used on a single field")
		}
		q.startAfter = append(q.startAfter, nil)
		q.q = q.q.OrderBy(path, firestore.Asc)
		return nil
	case *expr.Type_ListType_:
		// TODO(kagadar): Use `array-contains`
	case *expr.Type_MapType_:
		// TODO(kagadar): map differs from message maybe?
	}
	return status.Error(codes.InvalidArgument, "has must be used on a message, map or list")
}

func (q *query) transpileEquality(e *expr.Expr_Call, not bool) error {
	// TODO(kagadar): Implement
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
		// TODO(kagadar): Chain together with current query
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
		// TODO(kagadar): search all searchable fields
	default:
		// Unclear if other expressions can exist here.
		log.Printf("unexpected expression: %v", e)
	}
	return status.Error(codes.InvalidArgument, "invalid filter expression")
}

func (t transpiler) Transpile(ctx context.Context, req ListRequest) ([]*firestore.DocumentSnapshot, error) {
	pageSize := req.GetPageSize()
	switch {
	case pageSize < 0:
		return nil, status.Errorf(codes.InvalidArgument, "page size cannot be negative")
	case pageSize == 0:
		pageSize = t.defaultPageSize
	case req.GetPageSize() > t.maxPageSize:
		pageSize = t.maxPageSize
	}
	filter, err := filtering.ParseFilter(req, t.decls)
	if err != nil {
		return nil, err
	}
	q := &query{q: t.client.Collection(fmt.Sprintf("%s/%s", req.GetParent(), t.collection)).Query, types: filter.CheckedExpr.GetTypeMap()}
	err = q.transpile(filter.CheckedExpr.GetExpr(), false)
	if err != nil {
		return nil, err
	}
	if req.GetPageToken() != "" {
		q.q = q.q.OrderBy(firestore.DocumentID, firestore.Asc)
		q.startAfter = append(q.startAfter, req.GetPageToken())
	}
	if len(q.startAfter) > 0 {
		q.q = q.q.StartAfter(q.startAfter...)
	}
	return q.q.Documents(ctx).GetAll()
}
