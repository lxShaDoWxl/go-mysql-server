// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planbuilder

import (
	"fmt"
	"reflect"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func (b *Builder) buildUnion(inScope *scope, u *ast.Union) (outScope *scope) {
	leftScope := b.buildSelectStmt(inScope, u.Left)
	rightScope := b.buildSelectStmt(inScope, u.Right)

	distinct := u.Type != ast.UnionAllStr
	limit := b.buildLimit(inScope, u.Limit)
	offset := b.buildOffset(inScope, u.Limit)

	// mysql errors for order by right projection
	orderByScope := b.analyzeOrderBy(leftScope, leftScope, u.OrderBy)

	var sortFields sql.SortFields
	for _, c := range orderByScope.cols {
		so := sql.Ascending
		if c.descending {
			so = sql.Descending
		}
		scalar := c.scalar
		if scalar == nil {
			scalar = c.scalarGf()
		}
		// Unions pass order bys to the top scope, where the original
		// order by get field may not longer be accessible. Here it is
		// safe to assume the alias has already been computed.
		scalar, _, _ = transform.Expr(scalar, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.Alias:
				return expression.NewGetField(int(c.id), e.Type(), e.Name(), e.IsNullable()), transform.NewTree, nil
			default:
				return e, transform.SameTree, nil
			}
		})
		sf := sql.SortField{
			Column: scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}

	n, ok := leftScope.node.(*plan.Union)
	if ok {
		if len(n.SortFields) > 0 {
			if len(sortFields) > 0 {
				err := sql.ErrConflictingExternalQuery.New()
				b.handleErr(err)
			}
			sortFields = n.SortFields
		}
		if n.Limit != nil {
			if limit != nil {
				err := fmt.Errorf("conflicing external LIMIT")
				b.handleErr(err)
			}
			limit = n.Limit
		}
		if n.Offset != nil {
			if offset != nil {
				err := fmt.Errorf("conflicing external OFFSET")
				b.handleErr(err)
			}
			offset = n.Offset
		}
		leftScope.node = plan.NewUnion(n.Left(), n.Right(), n.Distinct, nil, nil, nil)
	}

	ret := plan.NewUnion(leftScope.node, rightScope.node, distinct, limit, offset, sortFields)
	outScope = leftScope
	outScope.node = b.mergeUnionSchemas(ret)
	return
}

func (b *Builder) mergeUnionSchemas(u *plan.Union) sql.Node {
	ls, rs := u.Left().Schema(), u.Right().Schema()
	if len(ls) != len(rs) {
		err := ErrUnionSchemasDifferentLength.New(len(ls), len(rs))
		b.handleErr(err)
	}
	les, res := make([]sql.Expression, len(ls)), make([]sql.Expression, len(rs))
	hasdiff := false
	var err error
	for i := range ls {
		les[i] = expression.NewGetFieldWithTable(i, ls[i].Type, ls[i].Source, ls[i].Name, ls[i].Nullable)
		res[i] = expression.NewGetFieldWithTable(i, rs[i].Type, rs[i].Source, rs[i].Name, rs[i].Nullable)
		if reflect.DeepEqual(ls[i].Type, rs[i].Type) {
			continue
		}
		hasdiff = true

		// try to get optimal type to convert both into
		convertTo := expression.GetConvertToType(ls[i].Type, rs[i].Type)

		// TODO: Principled type coercion...
		les[i], err = b.f.buildConvert(les[i], convertTo, 0, 0)
		res[i], err = b.f.buildConvert(res[i], convertTo, 0, 0)

		// Preserve schema names across the conversion.
		les[i] = expression.NewAlias(ls[i].Name, les[i])
		res[i] = expression.NewAlias(rs[i].Name, res[i])
	}
	var ret sql.Node = u
	if hasdiff {
		ret, err = u.WithChildren(
			plan.NewProject(les, u.Left()),
			plan.NewProject(res, u.Right()),
		)
		if err != nil {
			b.handleErr(err)
		}
	}
	return ret
}
