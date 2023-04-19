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

package sql

// Visitor visits expressions in an expression tree.
type VisitorWithError interface {
	// Visit method is invoked for each expr encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the expr with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	VisitWithError(expr Expression) (VisitorWithError, error)
}

// Walk traverses the expression tree in depth-first order. It starts by calling
// v.Visit(expr); expr must not be nil. If the visitor returned by
// v.Visit(expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil)
// to the returned visitor.
func WalkWithError(v VisitorWithError, expr Expression) error {
	v, err := v.VisitWithError(expr)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	for _, child := range expr.Children() {
		err = WalkWithError(v, child)
		if err != nil {
			return err
		}
	}
	return nil
}

// NodeVisitor visits expressions in an expression tree. Like Visitor, but with the added context of the node in which
// an expression is embedded. See WalkExpressionsWithNode in the plan package.
type NodeVisitorWithError interface {
	// Visit method is invoked for each expr encountered by Walk. If the result Visitor is not nil, Walk visits each of
	// the children of the expr with that visitor, followed by a call of Visit(nil, nil) to the returned visitor.
	VisitWithError(node Node, expression Expression) (NodeVisitorWithError, error)
}

// WalkWithNode traverses the expression tree in depth-first order. It starts by calling v.Visit(node, expr); expr must
// not be nil. If the visitor returned by v.Visit(node, expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil, nil) to the returned visitor.
func WalkWithNodeWithError(v NodeVisitorWithError, n Node, expr Expression) error {
	v, err := v.VisitWithError(n, expr)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	for _, child := range expr.Children() {
		err = WalkWithNodeWithError(v, n, child)
		if err != nil {
			return err
		}
	}

	_, err = v.VisitWithError(nil, nil)
	return err
}

type inspectorWithError func(Expression) (bool, error)

func (f inspectorWithError) VisitWithError(expr Expression) (VisitorWithError, error) {
	res, err := f(expr)
	if err != nil {
		return nil, err
	}
	if res {
		return f, nil
	}
	return nil, nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(expr); expr must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of expr, followed by a call of
// f(nil).
func InspectWithError(expr Expression, f func(expr Expression) (bool, error)) error {
	return WalkWithError(inspectorWithError(f), expr)
}

// NillaryWithChildren is a common implementation of expression.WithChildren for expressions with no children.
func NillaryWithChildren(expr Expression, children ...Expression) (Expression, error) {
	if len(children) > 0 {
		return nil, ErrInvalidChildrenNumber.New(expr, len(children), 0)
	}
	return expr, nil
}

// Visitor visits expressions in an expression tree.
type Visitor interface {
	// Visit method is invoked for each expr encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the expr with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(expr Expression) Visitor
}

// Walk traverses the expression tree in depth-first order. It starts by calling
// v.Visit(expr); expr must not be nil. If the visitor returned by
// v.Visit(expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil)
// to the returned visitor.
func Walk(v Visitor, expr Expression) {
	if v = v.Visit(expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		Walk(v, child)
	}
}

// NodeVisitor visits expressions in an expression tree. Like Visitor, but with the added context of the node in which
// an expression is embedded. See WalkExpressionsWithNode in the plan package.
type NodeVisitor interface {
	// Visit method is invoked for each expr encountered by Walk. If the result Visitor is not nil, Walk visits each of
	// the children of the expr with that visitor, followed by a call of Visit(nil, nil) to the returned visitor.
	Visit(node Node, expression Expression) NodeVisitor
}

// WalkWithNode traverses the expression tree in depth-first order. It starts by calling v.Visit(node, expr); expr must
// not be nil. If the visitor returned by v.Visit(node, expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil, nil) to the returned visitor.
func WalkWithNode(v NodeVisitor, n Node, expr Expression) {
	if v = v.Visit(n, expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		WalkWithNode(v, n, child)
	}

	v.Visit(nil, nil)
}

type inspector func(Expression) bool

func (f inspector) Visit(expr Expression) Visitor {
	if f(expr) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(expr); expr must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of expr, followed by a call of
// f(nil).
func Inspect(expr Expression, f func(expr Expression) bool) {
	Walk(inspector(f), expr)
}
