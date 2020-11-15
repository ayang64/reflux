package reads

import (
	"errors"
	"fmt"

	"github.com/ayang64/reflux/models"
	"github.com/ayang64/reflux/storage/reads/datatypes"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/semantic"
)

const (
	fieldKey       = "_field"
	measurementKey = "_measurement"
	valueKey       = "_value"
)

func toStoragePredicate(f *semantic.FunctionExpression) (*datatypes.Predicate, error) {
	if f.Block.Parameters == nil || len(f.Block.Parameters.List) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicateHelper(f.Block.Body.(semantic.Expression), f.Block.Parameters.List[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &datatypes.Predicate{
		Root: root,
	}, nil
}

func toStoragePredicateHelper(n semantic.Expression, objectName string) (*datatypes.Node, error) {
	switch n := n.(type) {
	case *semantic.LogicalExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, fmt.Errorf("%w: left hand side", err)
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, fmt.Errorf("%w: right hand side", err)
		}
		children := []*datatypes.Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalOr},
				Children: children,
			}, nil
		default:
			return nil, fmt.Errorf("unknown logical operator %v", n.Operator)
		}
	case *semantic.BinaryExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, fmt.Errorf("%w: left hand side", err)
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, fmt.Errorf("%w: right hand side", err)
		}
		children := []*datatypes.Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeComparisonExpression,
			Value:    &datatypes.Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		switch n.Property {
		case fieldKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: models.FieldKeyTagKey,
				},
			}, nil
		case measurementKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: models.MeasurementTagKey,
				},
			}, nil
		case valueKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeFieldRef,
				Value: &datatypes.Node_FieldRefValue{
					FieldRefValue: valueKey,
				},
			}, nil

		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeTagRef,
			Value: &datatypes.Node_TagRefValue{
				TagRefValue: n.Property,
			},
		}, nil
	case *semantic.DurationLiteral:
		return nil, errors.New("duration literals not supported in storage predicates")
	case *semantic.DateTimeLiteral:
		return nil, errors.New("time literals not supported in storage predicates")
	default:
		return nil, fmt.Errorf("unsupported semantic expression type %T", n)
	}
}

func toComparisonOperator(o ast.OperatorKind) (datatypes.Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return datatypes.ComparisonEqual, nil
	case ast.NotEqualOperator:
		return datatypes.ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return datatypes.ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return datatypes.ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return datatypes.ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return datatypes.ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return datatypes.ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return datatypes.ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return datatypes.ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}
