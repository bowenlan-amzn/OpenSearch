/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link SortedNumericDoubleValues} instance that wraps a {@link SortedNumericDocValues}
 * and converts the stored sortable long bits back to doubles using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 * <p>
 * This is used for {@code double} and {@code float} field types where the stored values
 * are IEEE 754 floating-point bits encoded as sortable longs. For example, the double
 * value {@code 3.14} is stored as the long {@code 4614253070214989087}.
 * <p>
 * This is different from {@link FieldData.SortedDoubleCastedValues} which handles
 * integer field types ({@code long}, {@code integer}, etc.) where the stored value
 * is the actual number and only needs a simple cast to double.
 *
 * @see FieldData#sortableLongBitsToDoubles(SortedNumericDocValues)
 * @opensearch.internal
 */
final class SortableLongBitsToSortedNumericDoubleValues extends SortedNumericDoubleValues {

    private final SortedNumericDocValues values;

    SortableLongBitsToSortedNumericDoubleValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public double nextValue() throws IOException {
        return NumericUtils.sortableLongToDouble(values.nextValue());
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getLongValues() {
        return values;
    }

    @Override
    public int advance(int target) throws IOException {
        return values.advance(target);
    }

    @Override
    public void doubleValues(int size, int[] docs, double[] output, double defaultValue) throws IOException {
        NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            // Single-valued: use Lucene's native bulk API (optimized in codec)
            long[] longBuffer = new long[size];
            singleton.longValues(size, docs, longBuffer, NumericUtils.doubleToSortableLong(defaultValue));
            for (int i = 0; i < size; i++) {
                output[i] = NumericUtils.sortableLongToDouble(longBuffer[i]);
            }
        } else {
            // Multi-valued: fall back to doc-by-doc
            for (int i = 0; i < size; i++) {
                if (values.advanceExact(docs[i])) {
                    output[i] = NumericUtils.sortableLongToDouble(values.nextValue());
                } else {
                    output[i] = defaultValue;
                }
            }
        }
    }
}
