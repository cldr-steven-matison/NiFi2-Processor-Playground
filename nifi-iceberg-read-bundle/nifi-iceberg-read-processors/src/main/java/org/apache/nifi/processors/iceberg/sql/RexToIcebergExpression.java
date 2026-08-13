/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.iceberg.sql;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates Calcite row expressions into equivalent Iceberg expressions. {@code translate}
 * returns {@code null} whenever a node (or any part of it) has no provably-equivalent Iceberg
 * form — the caller then leaves that node in Calcite's filter list and Calcite evaluates it as
 * a residual filter, so correctness never depends on this translator being complete.
 *
 * v1 scope: {@code = <> < <= > >= IS NULL IS NOT NULL IN} and prefix {@code LIKE}, combined with
 * {@code AND/OR/NOT}, over string, numeric, boolean and decimal columns compared to literals.
 * Anything else — date/time/timestamp predicates, functions, arithmetic, column-to-column —
 * stays residual. Negating forms ({@code <>}, {@code NOT}, {@code NOT IN}) are pushed only for
 * required (non-null) columns: SQL three-valued logic excludes NULL rows from a negated
 * predicate while Iceberg's negations include them, so on nullable columns they stay residual.
 */
public final class RexToIcebergExpression {

    private RexToIcebergExpression() {
    }

    public static Expression translate(final RexNode node, final List<String> fieldNames, final Schema schema) {
        try {
            return translateNode(node, fieldNames, schema);
        } catch (final RuntimeException | AssertionError e) {
            // any unexpected shape (odd literal conversion, exotic RexNode) simply stays residual
            return null;
        }
    }

    private static Expression translateNode(final RexNode node, final List<String> fieldNames, final Schema schema) {
        if (node instanceof RexInputRef ref) {
            // a bare boolean column used as a predicate, e.g. WHERE active
            final Types.NestedField field = fieldOf(ref, fieldNames, schema);
            return field != null && field.type().typeId() == Type.TypeID.BOOLEAN
                    ? Expressions.equal(field.name(), true) : null;
        }
        if (!(node instanceof RexCall call)) {
            return null;
        }
        return switch (node.getKind()) {
            case AND, OR -> translateComposite(call, fieldNames, schema);
            case NOT -> translateNot(call, fieldNames, schema);
            case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL ->
                    translateComparison(call, fieldNames, schema);
            case IS_NULL, IS_NOT_NULL -> translateNullCheck(call, fieldNames, schema);
            case LIKE -> translateLike(call, fieldNames, schema);
            case SEARCH -> translateSearch(call, fieldNames, schema);
            default -> null;
        };
    }

    private static Expression translateComposite(final RexCall call, final List<String> fieldNames, final Schema schema) {
        Expression result = null;
        for (final RexNode operand : call.getOperands()) {
            final Expression translated = translateNode(operand, fieldNames, schema);
            if (translated == null) {
                return null;
            }
            if (result == null) {
                result = translated;
            } else {
                result = call.getKind() == SqlKind.AND ? Expressions.and(result, translated) : Expressions.or(result, translated);
            }
        }
        return result;
    }

    private static Expression translateNot(final RexCall call, final List<String> fieldNames, final Schema schema) {
        final RexNode operand = call.getOperands().get(0);
        if (!allReferencedColumnsRequired(operand, fieldNames, schema)) {
            return null;
        }
        final Expression translated = translateNode(operand, fieldNames, schema);
        return translated == null ? null : Expressions.not(translated);
    }

    private static Expression translateComparison(final RexCall call, final List<String> fieldNames, final Schema schema) {
        final RexNode left = call.getOperands().get(0);
        final RexNode right = call.getOperands().get(1);

        final Types.NestedField field;
        final RexLiteral literal;
        SqlKind kind = call.getKind();
        final Types.NestedField leftField = unwrapField(left, fieldNames, schema, true);
        final Types.NestedField rightField = unwrapField(right, fieldNames, schema, true);
        if (leftField != null && right instanceof RexLiteral) {
            field = leftField;
            literal = (RexLiteral) right;
        } else if (rightField != null && left instanceof RexLiteral) {
            field = rightField;
            literal = (RexLiteral) left;
            kind = kind.reverse();
        } else {
            return null;
        }
        if (kind == SqlKind.NOT_EQUALS && field.isOptional()) {
            return null;
        }
        final Object value = literalValue(literal, field.type());
        if (value == null) {
            return null;
        }
        final String column = field.name();
        return switch (kind) {
            case EQUALS -> Expressions.equal(column, value);
            case NOT_EQUALS -> Expressions.notEqual(column, value);
            case LESS_THAN -> Expressions.lessThan(column, value);
            case LESS_THAN_OR_EQUAL -> Expressions.lessThanOrEqual(column, value);
            case GREATER_THAN -> Expressions.greaterThan(column, value);
            case GREATER_THAN_OR_EQUAL -> Expressions.greaterThanOrEqual(column, value);
            default -> null;
        };
    }

    private static Expression translateNullCheck(final RexCall call, final List<String> fieldNames, final Schema schema) {
        // a cast never changes nullness, so any cast wrapping the column is fine here
        final Types.NestedField field = unwrapField(call.getOperands().get(0), fieldNames, schema, false);
        if (field == null) {
            return null;
        }
        return call.getKind() == SqlKind.IS_NULL ? Expressions.isNull(field.name()) : Expressions.notNull(field.name());
    }

    private static Expression translateLike(final RexCall call, final List<String> fieldNames, final Schema schema) {
        // only the two-operand form; a custom ESCAPE clause stays residual
        if (call.getOperands().size() != 2 || !(call.getOperands().get(1) instanceof RexLiteral literal)) {
            return null;
        }
        final Types.NestedField field = unwrapField(call.getOperands().get(0), fieldNames, schema, true);
        if (field == null || field.type().typeId() != Type.TypeID.STRING) {
            return null;
        }
        final String pattern = literal.getValueAs(String.class);
        if (pattern == null) {
            return null;
        }
        if (!pattern.contains("%") && !pattern.contains("_")) {
            return Expressions.equal(field.name(), pattern);
        }
        if (pattern.endsWith("%")) {
            final String prefix = pattern.substring(0, pattern.length() - 1);
            if (!prefix.contains("%") && !prefix.contains("_")) {
                return Expressions.startsWith(field.name(), prefix);
            }
        }
        return null;
    }

    /**
     * Calcite normalizes IN lists and range conjunctions into SEARCH over a {@link Sarg};
     * point sets become {@code in}/{@code notIn} and ranges become and/or chains of bounds.
     */
    private static Expression translateSearch(final RexCall call, final List<String> fieldNames, final Schema schema) {
        if (!(call.getOperands().get(1) instanceof RexLiteral literal)) {
            return null;
        }
        final Types.NestedField field = unwrapField(call.getOperands().get(0), fieldNames, schema, true);
        if (field == null) {
            return null;
        }
        final Sarg<?> sarg = literal.getValueAs(Sarg.class);
        if (sarg == null) {
            return null;
        }
        final String column = field.name();

        Expression expression;
        if (sarg.isPoints()) {
            final List<Object> values = endpointValues(sarg.rangeSet.asRanges(), field.type());
            if (values == null) {
                return null;
            }
            expression = Expressions.in(column, values);
        } else if (sarg.isComplementedPoints()) {
            if (field.isOptional()) {
                return null;
            }
            final List<Object> values = endpointValues(sarg.rangeSet.complement().asRanges(), field.type());
            if (values == null) {
                return null;
            }
            expression = Expressions.notIn(column, values);
        } else {
            expression = null;
            for (final Range<?> range : sarg.rangeSet.asRanges()) {
                final Expression rangeExpression = translateRange(column, field.type(), range);
                if (rangeExpression == null) {
                    return null;
                }
                expression = expression == null ? rangeExpression : Expressions.or(expression, rangeExpression);
            }
            if (expression == null) {
                return null;
            }
        }

        if (sarg.nullAs == RexUnknownAs.TRUE) {
            expression = Expressions.or(Expressions.isNull(column), expression);
        }
        return expression;
    }

    private static Expression translateRange(final String column, final Type type, final Range<?> range) {
        Expression expression = null;
        if (range.hasLowerBound()) {
            final Object low = endpointValue(range.lowerEndpoint(), type);
            if (low == null) {
                return null;
            }
            expression = range.lowerBoundType() == BoundType.OPEN
                    ? Expressions.greaterThan(column, low) : Expressions.greaterThanOrEqual(column, low);
        }
        if (range.hasUpperBound()) {
            final Object high = endpointValue(range.upperEndpoint(), type);
            if (high == null) {
                return null;
            }
            final Expression upper = range.upperBoundType() == BoundType.OPEN
                    ? Expressions.lessThan(column, high) : Expressions.lessThanOrEqual(column, high);
            expression = expression == null ? upper : Expressions.and(expression, upper);
        }
        // a bound-less range means "any non-null value"
        return expression == null ? Expressions.notNull(column) : expression;
    }

    private static List<Object> endpointValues(final Iterable<? extends Range<?>> ranges, final Type type) {
        final List<Object> values = new ArrayList<>();
        for (final Range<?> range : ranges) {
            final Object value = endpointValue(range.lowerEndpoint(), type);
            if (value == null) {
                return null;
            }
            values.add(value);
        }
        return values;
    }

    /** Sarg endpoints arrive as Calcite internal values: NlsString for character types, BigDecimal for numerics. */
    private static Object endpointValue(final Comparable<?> endpoint, final Type type) {
        return switch (type.typeId()) {
            case STRING -> endpoint instanceof NlsString nlsString ? nlsString.getValue() : null;
            case BOOLEAN -> endpoint instanceof Boolean bool ? bool : null;
            case INTEGER, LONG, FLOAT, DOUBLE, DECIMAL ->
                    endpoint instanceof BigDecimal decimal ? numericValue(decimal, type) : null;
            default -> null;
        };
    }

    private static Object literalValue(final RexLiteral literal, final Type type) {
        if (literal.isNull()) {
            return null;
        }
        return switch (type.typeId()) {
            case BOOLEAN -> literal.getValueAs(Boolean.class);
            case STRING -> literal.getValueAs(String.class);
            case INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> numericValue(literal.getValueAs(BigDecimal.class), type);
            default -> null;
        };
    }

    /** Converts a Calcite numeric into the column's value type only when the conversion is exact — anything lossy stays residual. */
    private static Object numericValue(final BigDecimal decimal, final Type type) {
        if (decimal == null) {
            return null;
        }
        try {
            return switch (type.typeId()) {
                case INTEGER -> decimal.intValueExact();
                case LONG -> decimal.longValueExact();
                case DOUBLE -> decimal.doubleValue();
                case FLOAT -> {
                    final double asDouble = decimal.doubleValue();
                    final float asFloat = (float) asDouble;
                    yield (double) asFloat == asDouble ? asFloat : null;
                }
                case DECIMAL -> decimal;
                default -> null;
            };
        } catch (final ArithmeticException e) {
            return null;
        }
    }

    /**
     * Resolves an operand to the Iceberg column it references — either a bare input ref, or a
     * ref wrapped in a cast Calcite inserted for type coercion. When {@code valueSensitive}, the
     * cast is only accepted if it provably preserves the column's values (so pushing the
     * predicate against the uncast column is equivalent); truncating or lossy casts stay residual.
     */
    private static Types.NestedField unwrapField(final RexNode node, final List<String> fieldNames, final Schema schema,
                                                 final boolean valueSensitive) {
        if (node instanceof RexInputRef ref) {
            return fieldOf(ref, fieldNames, schema);
        }
        if (node instanceof RexCall call && call.getKind() == SqlKind.CAST
                && call.getOperands().get(0) instanceof RexInputRef ref) {
            final Types.NestedField field = fieldOf(ref, fieldNames, schema);
            if (field != null && (!valueSensitive || castPreservesValues(call.getType(), field.type()))) {
                return field;
            }
        }
        return null;
    }

    private static boolean castPreservesValues(final org.apache.calcite.rel.type.RelDataType target, final Type columnType) {
        final org.apache.calcite.sql.type.SqlTypeName name = target.getSqlTypeName();
        return switch (columnType.typeId()) {
            // a length-limited VARCHAR cast could truncate column values, so only the unlimited form is safe
            case STRING -> name == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
                    && (target.getPrecision() < 0 || target.getPrecision() == Integer.MAX_VALUE);
            case INTEGER -> name == org.apache.calcite.sql.type.SqlTypeName.INTEGER
                    || name == org.apache.calcite.sql.type.SqlTypeName.BIGINT
                    || (name == org.apache.calcite.sql.type.SqlTypeName.DECIMAL && target.getScale() == 0 && target.getPrecision() >= 10);
            case LONG -> name == org.apache.calcite.sql.type.SqlTypeName.BIGINT
                    || (name == org.apache.calcite.sql.type.SqlTypeName.DECIMAL && target.getScale() == 0 && target.getPrecision() >= 19);
            // every float is exactly representable as a double
            case FLOAT -> name == org.apache.calcite.sql.type.SqlTypeName.REAL
                    || name == org.apache.calcite.sql.type.SqlTypeName.FLOAT
                    || name == org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
            case DOUBLE -> name == org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
            case BOOLEAN -> name == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
            default -> false;
        };
    }

    private static Types.NestedField fieldOf(final RexInputRef ref, final List<String> fieldNames, final Schema schema) {
        if (ref.getIndex() >= fieldNames.size()) {
            return null;
        }
        return schema.findField(fieldNames.get(ref.getIndex()));
    }

    private static boolean allReferencedColumnsRequired(final RexNode node, final List<String> fieldNames, final Schema schema) {
        if (node instanceof RexInputRef ref) {
            final Types.NestedField field = fieldOf(ref, fieldNames, schema);
            return field != null && field.isRequired();
        }
        if (node instanceof RexCall call) {
            for (final RexNode operand : call.getOperands()) {
                if (!allReferencedColumnsRequired(operand, fieldNames, schema)) {
                    return false;
                }
            }
            return true;
        }
        return node instanceof RexLiteral;
    }
}
