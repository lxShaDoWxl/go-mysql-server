// Copyright 2020-2021 Dolthub, Inc.
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

package transform

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// Visitor visits nodes in the plan.
type VisitorWithError interface {
	// Visit method is invoked for each node encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the node with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	VisitWithError(node sql.Node) (VisitorWithError, error)
}

// Walk traverses the plan tree in depth-first order. It starts by calling v.Visit(node); node must not be nil. If the
// visitor returned by  v.Visit(node) is not nil, Walk is invoked recursively with the returned visitor for each
// children of the node, followed by a call of v.Visit(nil) to the returned visitor. If v.Visit(node) returns non-nil,
// then all children are walked, even if one of them returns nil for v.Visit().
func WalkWithError(v VisitorWithError, node sql.Node) error {
	v, err := v.VisitWithError(node)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	for _, child := range node.Children() {
		err = WalkWithError(v, child)
		if err != nil {
			return err
		}
	}

	v.VisitWithError(nil)
	return nil
}

type inspectorWithError func(sql.Node) (bool, error)

func (f inspectorWithError) VisitWithError(node sql.Node) (VisitorWithError, error) {
	res, err := f(node)
	if res {
		return f, err
	}
	return nil, err
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of node, followed by a call of
// f(nil).
func InspectWithError(node sql.Node, f func(sql.Node) (bool, error)) error {
	return WalkWithError(inspectorWithError(f), node)
}

// WalkExpressions traverses the plan and calls sql.Walk on any expression it finds.
func WalkExpressionsWithError(v sql.VisitorWithError, node sql.Node) error {
	return InspectWithError(node, func(node sql.Node) (bool, error) {
		if n, ok := node.(sql.Expressioner); ok {
			for _, e := range n.Expressions() {
				err := sql.WalkWithError(v, e)
				if err != nil {
					return false, err
				}
			}
		}
		return true, nil
	})
}

// WalkExpressionsWithNode traverses the plan and calls sql.WalkWithNode on any expression it finds.
func WalkExpressionsWithNodeWithError(v sql.NodeVisitorWithError, n sql.Node) {
	InspectWithError(n, func(n sql.Node) (bool, error) {
		if expressioner, ok := n.(sql.Expressioner); ok {
			for _, e := range expressioner.Expressions() {
				err := sql.WalkWithNodeWithError(v, n, e)
				if err != nil {
					return false, nil
				}
			}
		}
		return true, nil
	})
}

// InspectExpressions traverses the plan and calls sql.Inspect on any
// expression it finds.
func InspectExpressionsWithError(node sql.Node, f func(sql.Expression) (bool, error)) error {
	return WalkExpressionsWithError(exprInspectorWithError(f), node)
}

type exprInspectorWithError func(sql.Expression) (bool, error)

func (f exprInspectorWithError) VisitWithError(e sql.Expression) (sql.VisitorWithError, error) {
	res, err := f(e)
	if err != nil {
		return nil, err
	}
	if res {
		return f, nil
	}
	return nil, nil
}

// InspectExpressionsWithNode traverses the plan and calls sql.Inspect on any expression it finds.
func InspectExpressionsWithNodeWithError(node sql.Node, f func(sql.Node, sql.Expression) (bool, error)) {
	WalkExpressionsWithNodeWithError(exprWithNodeWithErrorInspector(f), node)
}

type exprWithNodeWithErrorInspector func(sql.Node, sql.Expression) (bool, error)

func (f exprWithNodeWithErrorInspector) VisitWithError(n sql.Node, e sql.Expression) (sql.NodeVisitorWithError, error) {
	res, err := f(n, e)
	if err != nil {
		return nil, err
	}
	if res {
		return f, nil
	}
	return nil, nil
}

// Visitor visits nodes in the plan.
type Visitor interface {
	// Visit method is invoked for each node encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the node with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(node sql.Node) Visitor
}

// Walk traverses the plan tree in depth-first order. It starts by calling v.Visit(node); node must not be nil. If the
// visitor returned by  v.Visit(node) is not nil, Walk is invoked recursively with the returned visitor for each
// children of the node, followed by a call of v.Visit(nil) to the returned visitor. If v.Visit(node) returns non-nil,
// then all children are walked, even if one of them returns nil for v.Visit().
func Walk(v Visitor, node sql.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, child := range node.Children() {
		Walk(v, child)
	}

	v.Visit(nil)
}

type inspector func(sql.Node) bool

func (f inspector) Visit(node sql.Node) Visitor {
	if f(node) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of node, followed by a call of
// f(nil).
func Inspect(node sql.Node, f func(sql.Node) bool) {
	Walk(inspector(f), node)
}

// WalkExpressions traverses the plan and calls sql.Walk on any expression it finds.
func WalkExpressions(v sql.Visitor, node sql.Node) {
	Inspect(node, func(node sql.Node) bool {
		if n, ok := node.(sql.Expressioner); ok {
			for _, e := range n.Expressions() {
				sql.Walk(v, e)
			}
		}
		return true
	})
}

// WalkExpressionsWithNode traverses the plan and calls sql.WalkWithNode on any expression it finds.
func WalkExpressionsWithNode(v sql.NodeVisitor, n sql.Node) {
	Inspect(n, func(n sql.Node) bool {
		if expressioner, ok := n.(sql.Expressioner); ok {
			for _, e := range expressioner.Expressions() {
				sql.WalkWithNode(v, n, e)
			}
		}
		return true
	})
}

// InspectExpressions traverses the plan and calls sql.Inspect on any
// expression it finds.
func InspectExpressions(node sql.Node, f func(sql.Expression) bool) {
	WalkExpressions(exprInspector(f), node)
}

type exprInspector func(sql.Expression) bool

func (f exprInspector) Visit(e sql.Expression) sql.Visitor {
	if f(e) {
		return f
	}
	return nil
}

// InspectExpressionsWithNode traverses the plan and calls sql.Inspect on any expression it finds.
func InspectExpressionsWithNode(node sql.Node, f func(sql.Node, sql.Expression) bool) {
	WalkExpressionsWithNode(exprWithNodeInspector(f), node)
}

type exprWithNodeInspector func(sql.Node, sql.Expression) bool

func (f exprWithNodeInspector) Visit(n sql.Node, e sql.Expression) sql.NodeVisitor {
	if f(n, e) {
		return f
	}
	return nil
}
