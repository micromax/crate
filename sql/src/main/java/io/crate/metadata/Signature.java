/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.base.Preconditions;
import io.crate.types.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;


/**
 * Static constructors for generating signature operators and argument type matchers to be used
 * with {@link FunctionResolver#getSignature}.
 */
public class Signature {


    public interface SignatureOperator extends UnaryOperator<List<DataType>> {

        /**
         * Adds a precondition before the operator which should evaluate to true in order for the operator
         * to be evaluated.
         *
         * @param predicate a predicate which represents the precondition
         * @return a new operator with the predicate prepended
         */
        default SignatureOperator preCondition(Predicate<List<DataType>> predicate) {
            return dataTypes -> predicate.test(dataTypes) ? this.apply(dataTypes) : null;
        }

        /**
         * Adds a fallback to this operator. When this operator fails to match, the given fallback operator will
         * be evaluated.
         *
         * @param fallback the fallback operator
         * @return a new operator with the fallback appended
         */
        default SignatureOperator or(SignatureOperator fallback) {
            return dataTypes -> {
                List<DataType> n = this.apply(dataTypes);
                if (n == null) {
                    return fallback.apply(dataTypes);
                }
                return n;
            };
        }

        /**
         * Adds a secondary operator to this operator, which is executed with the result of this operator if a match
         * occurs.
         *
         * @param secondary the operator which will be evaluated with this operator's success result
         * @return a new operator with the secondary appended
         */
        default SignatureOperator and(SignatureOperator secondary) {
            return dataTypes -> {
                List<DataType> n = this.apply(dataTypes);
                if (n != null) {
                    return secondary.apply(n);
                }
                return null;
            };
        }

    }

    public static final SignatureOperator EMPTY = dataTypes -> dataTypes.isEmpty() ? dataTypes : null;

    public static final SignatureOperator SIGNATURES_SINGLE_NUMERIC = of(ArgMatcher.NUMERIC);
    public static final SignatureOperator SIGNATURES_SINGLE_ANY = of(1);

    public static final Predicate<DataType> IS_NULL = DataTypes.UNDEFINED::equals;


    public interface ArgMatcher extends UnaryOperator<DataType> {

        static ArgMatcher of(DataType... allowedTypes) {
            return dt -> {
                for (DataType allowedType : allowedTypes) {
                    if (allowedType.equals(dt)) {
                        return dt;
                    }
                }
                return IS_NULL.test(dt) ? dt : null;
            };
        }

        @SafeVarargs
        static ArgMatcher of(final Predicate<DataType>... predicates) {
            return dt -> {
                for (Predicate<DataType> predicate : predicates) {
                    if (predicate.test(dt)) {
                        return dt;
                    }
                }
                return IS_NULL.test(dt) ? dt : null;
            };
        }

        static ArgMatcher rewriteTo(final DataType targetType) {
            return dt -> {
                if (targetType.equals(dt) || IS_NULL.test(dt)) {
                    return targetType;
                }
                return null;
            };
        }


        ArgMatcher ANY = dt -> dt;
        ArgMatcher NUMERIC = of(DataTypes.NUMERIC_PRIMITIVE_TYPES::contains);

        ArgMatcher ANY_ARRAY = of(dt -> dt instanceof ArrayType);
        ArgMatcher ANY_SET = of(dt -> dt instanceof SetType);
        ArgMatcher ANY_COLLECTION = of(dt -> dt instanceof CollectionType);

        ArgMatcher STRING = rewriteTo(DataTypes.STRING);
        ArgMatcher BOOLEAN = rewriteTo(DataTypes.BOOLEAN);
        ArgMatcher INTEGER = rewriteTo(DataTypes.INTEGER);
        ArgMatcher OBJECT = rewriteTo(DataTypes.OBJECT);

    }

    /**
     * Creates an operator which takes its matchers from an iterator. The size of the
     * validated arguments is not checked, however it needs to be greater or equal to the
     * number of matchers returned by the iterator;
     */
    public static SignatureOperator of(Iterable<ArgMatcher> argMatchers) {
        return new IterableArgs(argMatchers);
    }

    public static SignatureOperator of(ArgMatcher... matchers) {
        if (matchers.length == 0) {
            return EMPTY;
        }
        return new VarArgs(matchers);
    }

    public static SignatureOperator size(int minSize, int maxSize) {
        Preconditions.checkArgument(minSize <= maxSize, "minSize needs to be smaller than maxSize");
        return dataTypes -> (dataTypes.size() >= minSize && dataTypes.size() <= maxSize) ? dataTypes : null;
    }


    public static SignatureOperator of(boolean hasVarArgs, boolean strictVarArgTypes, ArgMatcher... matchers) {
        return new VarArgs(hasVarArgs, strictVarArgTypes, matchers);
    }


    public static SignatureOperator of(int numArgs) {
        Preconditions.checkArgument(numArgs >= 0, "numArgs must not be negative");
        if (numArgs == 0) {
            return EMPTY;
        }
        return in -> in.size() == numArgs ? in : null;
    }

    public static SignatureOperator of(DataType... dataTypes) {
        if (dataTypes.length == 0) {
            return EMPTY;
        }
        ArgMatcher[] matchers = new ArgMatcher[dataTypes.length];
        for (int i = 0; i < matchers.length; i++) {
            matchers[i] = ArgMatcher.rewriteTo(dataTypes[i]);
        }
        return new VarArgs(matchers);
    }

    public static SignatureOperator of(List<DataType> dataTypes) {
        if (dataTypes.isEmpty()) {
            return EMPTY;
        }
        ArgMatcher[] matchers = new ArgMatcher[dataTypes.size()];
        for (int i = 0; i < matchers.length; i++) {
            matchers[i] = ArgMatcher.rewriteTo(dataTypes.get(i));
        }
        return new VarArgs(matchers);
    }


    /**
     * Runs matchers consumed from an iterator
     */
    private static class IterableArgs implements SignatureOperator {

        private final Iterable<ArgMatcher> expected;

        private IterableArgs(Iterable<ArgMatcher> expected) {
            this.expected = expected;
        }

        @Override
        public List<DataType> apply(List<DataType> dataTypes) {
            Iterator<ArgMatcher> iter = expected.iterator();
            for (DataType dataType : dataTypes) {
                if (!iter.hasNext()) return null;
                DataType repl = iter.next().apply(dataType);
                if (repl == null) {
                    return null;
                } else if (repl != dataType) {
                    return copy(dataTypes);
                }
            }
            return dataTypes;
        }

        private List<DataType> copy(List<DataType> in) {
            ArrayList<DataType> out = new ArrayList<>(in.size());
            Iterator<ArgMatcher> iter = expected.iterator();
            for (DataType dataType : in) {
                if (!iter.hasNext()) return null;
                DataType repl = iter.next().apply(dataType);
                if (repl == null) {
                    return null;
                }
                out.add(repl);
            }
            return out;
        }
    }

    private static class VarArgs implements Signature.SignatureOperator {

        private final ArgMatcher[] matchers;
        private final boolean varArgs;
        private final boolean checkVarArgTypes;

        private VarArgs(ArgMatcher[] matchers) {
            this(false, false, matchers);
        }

        private VarArgs(boolean hasVarargs, boolean checkVarArgTypes, ArgMatcher[] matchers) {
            assert matchers.length > 0 : "VarArgs requires at least one matcher";
            this.matchers = matchers;
            this.varArgs = hasVarargs;
            this.checkVarArgTypes = checkVarArgTypes;
        }

        /**
         * checks if all non null types in the given list are the same, beginning @start index
         */
        private DataType getCommonType(List<DataType> dataTypes, int start) {
            DataType commonType = null;
            for (int i = start; i < dataTypes.size(); i++) {
                DataType dataType = dataTypes.get(i);
                if (IS_NULL.test(dataType)) {
                    continue;
                }
                if (commonType == null) {
                    commonType = dataType;
                } else if (!commonType.equals(dataType)) {
                    return null;
                }
            }
            return commonType == null ? DataTypes.UNDEFINED : commonType;
        }

        @Override
        public List<DataType> apply(List<DataType> dataTypes) {
            ArgMatcher varArgMatcher = matchers[matchers.length - 1];
            if (dataTypes.size() != matchers.length) {
                if (varArgs && dataTypes.size() > matchers.length) {
                    if (checkVarArgTypes) {
                        DataType commonType = getCommonType(dataTypes, matchers.length - 1);
                        if (commonType == null) {
                            return null;
                        } else if (!IS_NULL.test(commonType)) {
                            // enforce the common type in varArgs
                            varArgMatcher = dt -> commonType;
                        }
                    }
                } else {
                    return null;
                }
            }

            for (int i = 0; i < dataTypes.size(); i++) {
                ArgMatcher matcher = i < matchers.length - 1 ? matchers[i] : varArgMatcher;
                DataType dataType = dataTypes.get(i);
                DataType repl = matcher.apply(dataType);
                if (repl == null) {
                    return null;
                } else if (repl != dataType) {
                    return copy(dataTypes, varArgMatcher);
                }
            }
            return dataTypes;
        }

        private List<DataType> copy(List<DataType> in, ArgMatcher varArgMatcher) {
            ArrayList<DataType> out = new ArrayList<>(in.size());
            for (int i = 0; i < in.size(); i++) {
                ArgMatcher matcher = i < matchers.length - 1 ? matchers[i] : varArgMatcher;
                DataType dt = matcher.apply(in.get(i));
                if (dt == null) {
                    return null;
                }
                out.add(dt);
            }
            return out;
        }

    }

    public static final SignatureOperator SIGNATURES_ALL_OF_SAME = of(true, true, ArgMatcher.ANY);
}
