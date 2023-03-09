// Copyright 2022 Dolthub, Inc.
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

package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type aliasDisambiguator struct {
	node                sql.Node
	aliases             TableAliases
	disambiguationIndex int
}

func (ad *aliasDisambiguator) GetAliases() (TableAliases, error) {
	if ad.aliases == nil {
		aliases, err := getTableAliases(ad.node, nil)
		if err != nil {
			return nil, err
		}
		ad.aliases = aliases
	}
	return ad.aliases, nil
}

func (ad *aliasDisambiguator) Disambiguate(alias string) (string, error) {
	nodeAliases, err := ad.GetAliases()
	if err != nil {
		return "", err
	}

	// all renamed aliases will be of the form <alias>_<disambiguationIndex++>
	for {
		ad.disambiguationIndex++
		aliasName := fmt.Sprintf("%s_%d", alias, ad.disambiguationIndex)
		if _, ok := nodeAliases[aliasName]; !ok {
			return aliasName, nil
		}
	}
}

func newAliasDisambiguator(node sql.Node, scope *Scope) *aliasDisambiguator {
	return &aliasDisambiguator{node: node}
}

// hoistSelectExists merges a WHERE EXISTS subquery scope with its outer
// scope when the subquery filters on columns from the outer scope.
//
// Consider this (NOT) EXISTS plan tree:
//
//	Filter
//	├─ (NOT) EXISTS Subquery
//	│  └─ Filter
//	│     ├─ (CONDITION)
//	│     └─ RIGHT
//	└─ LEFT
//
// There are a few cases to consider, all around `CONDITION`. Here are the various possibilities and the equivalent plans.
// Note that for each case we have plan #N (1N, 2N, 3N, 4N) for the cases where the subquery is NOT EXISTS.
//
//			Case 1: `CONDITION` depends on `LEFT` AND `RIGHT`
//		  		1. Need to scan both Relations
//			  	2. Convert 1 to `SemiJoin(LEFT, RIGHT, CONDITION)`
//	            3. Convert 1N to `AntiJoin(LEFT, RIGHT, CONDITION)`
//			Case 2: `CONDITION` depends on only `LEFT`
//			  	1. No need to scan RIGHT, just need to know if there are any rows in RIGHT
//		  		2. Convert 2 to `Proj(LEFT, CrossJoin(Filter(LEFT, CONDITION), Limit1(RIGHT)))`
//	            3. Convert 2N to `Proj(LEFT, CrossJoin(Filter(LEFT, NOT(CONDITION)), Limit1(RIGHT)))`
//			Case 3: `CONDITION` depends on only `RIGHT`
//				1. Need to scan both Relations
//			  	2. Convert 3 to	`Proj(LEFT, CrossJoin(LEFT, Limit1(Filter(RIGHT, CONDITION))))`
//	            3. Convert 3N to `AntiJoin(LEFT, Limit1(Filter(RIGHT, CONDITION)), CONDITION)`
//			Case 4: `CONDITION` DOES NOT depend on `LEFT` OR `RIGHT`
//			  	1. No need to scan RIGHT
//		  		2. Convert 4 to `Filter(LEFT, CONDITION)`
//	            3. Convert 4N to `Filter(LEFT, NOT(CONDITION))`
//
// If hoisting results in naming conflicts, we will rename the conflicting aliases/tables in the subquery.
// (Renaming tables/alias in a WHERE EXISTS subquery is perfectly safe, since the subquery results are never actually used.)
func hoistSelectExists(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	sel RuleSelector,
) (sql.Node, transform.TreeIdentity, error) {
	debug := sql.DebugString(n)
	a.Log("examining node: \n %s \n", debug)
	aliasDisambig := newAliasDisambiguator(n, scope)

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		f, ok := n.(*plan.Filter)
		if !ok {
			return n, transform.SameTree, nil
		}
		return hoistExistSubqueries(scope, a, f, aliasDisambig)
	})
}

// hoistExistSubqueries looks for a filter WHERE EXISTS, and then attempts to extract the subquery.
func hoistExistSubqueries(scope *Scope, a *Analyzer, filter *plan.Filter, aliasDisambig *aliasDisambiguator) (sql.Node, transform.TreeIdentity, error) {
	filterDebug := sql.DebugString(filter)
	a.Log("examining filter node: \n %s \n", filterDebug)

	exp := filter.Expression
	not, isNot := exp.(*expression.Not)
	if isNot {
		exp = not.Child
	}

	subquery, ok := exp.(*plan.ExistsSubquery)
	if !ok {
		return filter, transform.SameTree, nil
	}

	hoistInfo := analyzeExistsSubquery(a, filter, subquery)
	if hoistInfo == nil {
		return filter, transform.SameTree, nil
	}

	err := renameRightIfNecessary(a, hoistInfo, aliasDisambig, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	left := hoistInfo.left
	right := hoistInfo.right
	condition := hoistInfo.condition
	var result sql.Node

	switch {
	// case 1: condition uses columns from both sides
	//	Convert to `SemiJoin(LEFT, RIGHT, CONDITION)`
	case hoistInfo.referencesLeft && hoistInfo.referencesRight:
		if isNot {
			result = plan.NewAntiJoin(left, right, condition)
		} else {
			result = plan.NewSemiJoin(left, right, condition)
		}
	// case 2: condition uses columns from left side only
	//  Convert to `Proj(LEFT, CrossJoin(Filter(LEFT, CONDITION), Limit1(RIGHT)))`
	case hoistInfo.referencesLeft && !hoistInfo.referencesRight:
		var leftFilter sql.Node
		if isNot {
			leftFilter = plan.NewFilter(expression.NewNot(condition), left)
		} else {
			leftFilter = plan.NewFilter(condition, left)
		}
		result =
			plan.NewProject(
				expression.SchemaToGetFields(left.Schema()),
				plan.NewCrossJoin(
					leftFilter,
					plan.NewLimit(
						expression.NewLiteral(int8(1), types.Int8),
						right)))
	// case 3: condition uses columns from right side only
	//  Convert to `Proj(LEFT, CrossJoin(LEFT, Limit1(Filter(RIGHT, CONDITION))))`
	case !hoistInfo.referencesLeft && hoistInfo.referencesRight:
		rightSide := plan.NewLimit(
			expression.NewLiteral(int8(1), types.Int8),
			plan.NewFilter(
				condition,
				right))
		if isNot {
			result = plan.NewAntiJoin(left, rightSide, condition)
		} else {
			result = plan.NewProject(expression.SchemaToGetFields(left.Schema()),
				plan.NewCrossJoin(left, rightSide))
		}
	// case 4: condition uses no columns from either side
	//  Convert to `Filter(LEFT, CONDITION)`
	case !hoistInfo.referencesLeft && !hoistInfo.referencesRight:
		if isNot {
			condition = expression.NewNot(condition)
		}
		result = plan.NewFilter(condition, left)
	}

	return result, transform.NewTree, nil
}

func renameRightIfNecessary(a *Analyzer, hoistInfo *hoistAnalysis, aliasDisambig *aliasDisambiguator, scope *Scope) error {
	tryRenameRight := hoistInfo.referencesRight || hoistInfo.referencesLeft
	tryRenameCondition := hoistInfo.referencesRight
	if !tryRenameRight && !tryRenameCondition {
		return nil
	}
	right := hoistInfo.right
	condition := hoistInfo.condition

	// get outside aliases
	outsideAliases, err := aliasDisambig.GetAliases()
	if err != nil {
		return err
	}
	// get right aliases
	rightAliases, err := getTableAliases(right, scope)
	if err != nil {
		return err
	}

	// find conflicting and non-conflicting aliases
	conflicted, nonConflicted := outsideAliases.findConflicts(rightAliases)

	// add non-conflicting aliases to the outside scope
	for _, alias := range nonConflicted {
		target, ok := rightAliases[alias]
		if !ok {
			panic(fmt.Sprintf("alias %s not found", alias))
		}
		err = outsideAliases.add(fakeNameable{name: alias}, target)
		if err != nil {
			return err
		}
	}

	// disambiguate conflicting aliases
	var same transform.TreeIdentity
	for _, alias := range conflicted {
		// conflict, need to get a new alias
		newAlias, err := aliasDisambig.Disambiguate(alias)
		if err != nil {
			return err
		}
		// rename the alias in the right subtree
		right, same, err = renameAliases(right, alias, newAlias)
		if err != nil {
			return err
		}
		if same {
			return fmt.Errorf("tree is unchanged after trying to rename alias %s to %s in subtree %s",
				alias, newAlias, sql.DebugString(right))
		}

		// rename the alias in the condition, if necessary
		if tryRenameCondition {
			condition, same, err = renameAliasesInExp(condition, alias, newAlias)
			if err != nil {
				return err
			}
			if same {
				return fmt.Errorf("tree is unchanged after trying to rename alias %s to %s in expression %s",
					alias, newAlias, condition.String())
			}
		}

		// get updated aliases from the right subtree
		newAliases, err := getTableAliases(right, scope)
		if err != nil {
			return err
		}
		// find new renamed target from the right subtree
		target, ok := newAliases[newAlias]
		if !ok {
			panic(fmt.Sprintf("alias %s not found", newAlias))
		}
		// add the new alias to the outside scope
		err = outsideAliases.add(fakeNameable{name: newAlias}, target)
		if err != nil {
			return err
		}
	}

	hoistInfo.right = right
	hoistInfo.condition = condition

	return nil
}

type hoistAnalysis struct {
	left            sql.Node
	right           sql.Node
	condition       sql.Expression
	referencesLeft  bool
	referencesRight bool
}

type fakeNameable struct {
	name string
}

var _ sql.Nameable = (*fakeNameable)(nil)

func (f fakeNameable) Name() string { return f.name }

func gatherTables(n sql.Node) []string {
	var tables []string
	transform.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.TableAlias:
			tables = append(tables, n.Name())
		case *plan.ResolvedTable:
			tables = append(tables, n.Name())
		}
		return true
	})
	return tables
}
func isInListOfTables(tables []string, table string) bool {
	for _, t := range tables {
		if t == table {
			return true
		}
	}
	return false
}
func analyzeExistsSubquery(a *Analyzer, filter *plan.Filter, subquery *plan.ExistsSubquery) *hoistAnalysis {
	// find the first filter in the subquery
	var firstSubqueryFilter *plan.Filter
	var tracker = subquery.Query.Query
	for tracker != nil {
		children := tracker.Children()
		if len(children) != 1 {
			return nil
		}
		child := children[0]
		switch child := child.(type) {
		case *plan.Filter:
			firstSubqueryFilter = child
			tracker = nil
			break
		case *plan.Project:
			tracker = child
		default:
			return nil
		}
	}

	// if there is no filter, we can't hoist
	if firstSubqueryFilter == nil {
		return nil
	}

	left := filter.Child
	condition := firstSubqueryFilter.Expression
	right := firstSubqueryFilter.Child

	// figure out which side of the join the condition references
	referencesLeft := false
	referencesRight := false
	rightTables := gatherTables(right)
	leftTables := gatherTables(left)
	transform.InspectExpr(condition, func(e sql.Expression) bool {
		gf, ok := e.(*expression.GetField)
		if ok {
			source := gf.Table()
			if isInListOfTables(leftTables, source) {
				referencesLeft = true
			}
			if isInListOfTables(rightTables, source) {
				referencesRight = true
			}
		}
		return false
	})

	return &hoistAnalysis{
		left,
		right,
		condition,
		referencesLeft,
		referencesRight,
	}
}
