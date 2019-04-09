/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils.streamhist;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Test;
import org.quicktheories.core.Gen;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

public class StreamingTombstoneHistogramBuilderTest
{
    @Test
    public void testFunction() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9, 32, 30, 45 };

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < 7; i++)
        {
            builder.update(samples[i]);
        }

        // should end up (2,1),(9.5,2),(17.5,2),(23,1),(36,1)
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(9.0, 2L);
        expected1.put(17.0, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        TombstoneHistogram hist = builder.build();
        hist.forEach((point, value) ->
                     {
                         Map.Entry<Double, Long> entry = expectedItr.next();
                         assertEquals(entry.getKey(), point, 0.01);
                         assertEquals(entry.getValue().longValue(), value);
                     });

        // sum test
        assertEquals(3.5, hist.sum(15), 0.01);
        // sum test (b > max(hist))
        assertEquals(7.0, hist.sum(50), 0.01);
    }

    @Test
    public void testSerDe() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9 };

        // add 7 points to histogram of 5 bins
        for (int i = 0; i < samples.length; i++)
        {
            builder.update(samples[i]);
        }
        TombstoneHistogram hist = builder.build();
        DataOutputBuffer out = new DataOutputBuffer();
        TombstoneHistogram.serializer.serialize(hist, out);
        byte[] bytes = out.toByteArray();

        TombstoneHistogram deserialized = TombstoneHistogram.serializer.deserialize(new DataInputBuffer(bytes));

        // deserialized histogram should have following values
        Map<Double, Long> expected1 = new LinkedHashMap<Double, Long>(5);
        expected1.put(2.0, 1L);
        expected1.put(9.0, 2L);
        expected1.put(17.0, 2L);
        expected1.put(23.0, 1L);
        expected1.put(36.0, 1L);

        Iterator<Map.Entry<Double, Long>> expectedItr = expected1.entrySet().iterator();
        deserialized.forEach((point, value) ->
                             {
                                 Map.Entry<Double, Long> entry = expectedItr.next();
                                 assertEquals(entry.getKey(), point, 0.01);
                                 assertEquals(entry.getValue().longValue(), value);
                             });
    }


    @Test
    public void testNumericTypes() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);

        builder.update(2);
        builder.update(2);
        builder.update(2);
        TombstoneHistogram hist = builder.build();
        Map<Integer, Integer> asMap = asMap(hist);

        assertEquals(1, asMap.size());
        assertEquals(3, asMap.get(2).intValue());

        //Make sure it's working with Serde
        DataOutputBuffer out = new DataOutputBuffer();
        TombstoneHistogram.serializer.serialize(hist, out);
        byte[] bytes = out.toByteArray();

        TombstoneHistogram deserialized = TombstoneHistogram.serializer.deserialize(new DataInputBuffer(bytes));

        asMap = asMap(deserialized);
        assertEquals(1, deserialized.size());
        assertEquals(3, asMap.get(2).intValue());
    }

    @Test
    public void testOverflow() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 1);
        int[] samples = new int[]{ 23, 19, 10, 16, 36, 2, 9, 32, 30, 45, 31,
                                   32, 32, 33, 34, 35, 70, 78, 80, 90, 100,
                                   32, 32, 33, 34, 35, 70, 78, 80, 90, 100
        };

        // Hit the spool cap, force it to make bins
        for (int i = 0; i < samples.length; i++)
        {
            builder.update(samples[i]);
        }

        assertEquals(5, builder.build().size());
    }

    @Test
    public void testRounding() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 60);
        int[] samples = new int[]{ 59, 60, 119, 180, 181, 300 }; // 60, 60, 120, 180, 240, 300
        for (int i = 0; i < samples.length; i++)
            builder.update(samples[i]);
        TombstoneHistogram hist = builder.build();
        assertEquals(hist.size(), 5);
        assertEquals(asMap(hist).get(60).intValue(), 2);
        assertEquals(asMap(hist).get(120).intValue(), 1);
    }

    @Test
    public void testLargeValues() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 0, 1);
        IntStream.range(Integer.MAX_VALUE - 30, Integer.MAX_VALUE).forEach(builder::update);
    }

    private Map<Integer, Integer> asMap(TombstoneHistogram histogram)
    {
        Map<Integer, Integer> result = new HashMap<>();
        histogram.forEach(result::put);
        return result;
    }

    @Test
    public void testQT() throws Exception {
        qt().forAll(
                genStreamingTombstoneHistogramBuilder(),
                lists().of(integers().allPositive()).ofSize(100),
                lists().of(integers().allPositive()).ofSize(100))
        .checkAssert(this::addAndVerify);
    }

    @Test
    public void testQTFail1() throws Exception {
        StreamingTombstoneHistogramBuilder b = new StreamingTombstoneHistogramBuilder(1, 5, 60);

        List<Integer> keys = Arrays.asList(
             1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                 1, 244, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 182, 1, 1, 1, 1, 304, 1, 1, 1, 1, 1, 1,
                 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 485, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                 1, 1, 1, 1, 363, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 425, 127, 1, 1,
                 61, 1, 61
                );
        List<Integer> values = Arrays.asList(
                19, 3639, 861681, 34622, 161984, 44342048, 69, 75324, 222811048,
                  3312861, 3955, 50556, 422, 31596, 1, 1624019, 16576, 2806, 721, 509687, 584,
                  411819, 3320, 20924839, 3377, 5, 12, 491, 3408, 175187264, 1, 436652529,
                  67400974, 51586, 1, 10969, 13, 46758, 8910928, 7468, 391179, 1665445,
                  1100393, 87976, 13100, 14070, 2212, 4420225, 5597, 5, 196457, 995042260,
                  5662902, 225536, 17, 1554, 1, 29352461, 137820, 1, 183, 17840, 7791288,
                  1701086, 52281081, 173, 298, 377268, 3165, 2982, 29166574, 2439726, 5680,
                  569721, 1794, 7118, 3862461, 5, 132403, 1, 1652191, 71434, 14441758, 4698, 5,
                  867, 37, 3, 1, 30, 65904, 108374, 21621, 9747486, 1202, 4643, 954632, 264461,
                  1, 4263
                );

        addAndVerify(b, keys, values);
    }

    private void addAndVerify(StreamingTombstoneHistogramBuilder b, List<Integer> keys, List<Integer> values) {
        for (int i = 0; i < keys.size(); i++) {
            b.update(keys.get(i), values.get(i));
        }
        TombstoneHistogram hist = b.build();
        Map<Integer, Integer> map = asMap(hist);
        System.err.println(map);
    }

    private static Gen<StreamingTombstoneHistogramBuilder> genStreamingTombstoneHistogramBuilder() {
        Gen<Integer> bs = integers().from(1).upTo(1000);
        Gen<Integer> ss = integers().from(1).upTo(300000);
        return bs.zip(ss, (b, s) -> new StreamingTombstoneHistogramBuilder(b, s, 60));
    }
}
