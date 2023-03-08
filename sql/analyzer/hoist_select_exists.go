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
	n                   sql.Node
	scope               *Scope
	aliases             TableAliases
	disambiguationIndex int
}

func (ad *aliasDisambiguator) GetAliases() (TableAliases, error) {
	if ad.aliases == nil {
		aliases, err := getTableAliases(ad.n, ad.scope)
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

func newAliasDisambiguator(n sql.Node, scope *Scope) *aliasDisambiguator {
	return &aliasDisambiguator{n: n, scope: scope}
}

// hoistSelectExists merges a WHERE EXISTS subquery scope with its outer
// scope when the subquery filters on columns from the outer scope.
//
// Consider this plan tree:
//
//	Filter
//	├─ EXISTS Subquery
//	│  └─ Filter
//	│     ├─ (CONDITION)
//	│     └─ RIGHT
//	└─ LEFT
//
// There are a few cases to consider, all around `CONDITION`. Here are the various possibilities and the equivalent plan.
//
//		Case 1: `CONDITION` depends on `LEFT` AND `RIGHT`
//	  		1. Need to scan both Relations
//		  	2. Convert to `SemiJoin(LEFT, RIGHT, CONDITION)`
//		Case 2: `CONDITION` depends on only `LEFT`
//		  	1. No need to scan RIGHT, just need to know if there are any rows in RIGHT
//	  		2. Convert to `Proj(LEFT, CrossJoin(Filter(LEFT, CONDITION), Limit1(RIGHT)))`
//		Case 3: `CONDITION` depends on only `RIGHT`
//			1. Need to scan both Relations
//		  	2. Convert to `Proj(LEFT, CrossJoin(LEFT, Limit1(Filter(RIGHT, CONDITION))))`
//		Case 4: `CONDITION` DOES NOT depend on `LEFT` OR `RIGHT`
//		  	1. No need to scan RIGHT
//	  		2. Convert to `Filter(LEFT, CONDITION)`
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
	exp := filter.Expression
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
	case hoistInfo.referencesLeft && hoistInfo.referencesRight:
		result = plan.NewSemiJoin(
			left,
			right,
			condition)
	// case 2: condition uses columns from left side only
	case hoistInfo.referencesLeft && !hoistInfo.referencesRight:
		result =
			plan.NewProject(
				expression.SchemaToGetFields(left.Schema()),
				plan.NewCrossJoin(
					plan.NewFilter(condition, left),
					right))
	// case 3: condition uses columns from right side only
	case !hoistInfo.referencesLeft && hoistInfo.referencesRight:
		result =
			plan.NewProject(
				expression.SchemaToGetFields(left.Schema()),
				plan.NewCrossJoin(
					left,
					plan.NewLimit(
						expression.NewLiteral(int8(1), types.Int8),
						plan.NewFilter(
							condition,
							right))))
	// case 4: condition uses no columns from either side
	case !hoistInfo.referencesLeft && !hoistInfo.referencesRight:
		result = plan.NewFilter(condition, left)
	}

	return result, transform.NewTree, nil
}

func renameRightIfNecessary(a *Analyzer, hoistInfo *hoistAnalysis, aliasDisambig *aliasDisambiguator, scope *Scope) error {
	tryRenameRight := hoistInfo.referencesRight || hoistInfo.referencesLeft
	tryRenameCondition := hoistInfo.referencesRight
	if tryRenameRight {
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
	}

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
