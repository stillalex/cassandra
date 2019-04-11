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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Test;
import org.quicktheories.core.Gen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
        int size = 100;
        // qt v 0.26 has #withTestingTime to cap run time
        int examples = 3000; // -1 for infinite runs

        qt()
        .withExamples(examples)
        .forAll(
                genStreamingTombstoneHistogramBuilder(),
                lists().of(integers().allPositive()).ofSize(size),
                lists().of(integers().allPositive()).ofSize(size))
        .checkAssert(this::addAndVerify);
    }

    @Test
    public void testQTFail1() throws Exception { // overflow on sum
        List<Integer> keys = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                244, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 182, 1, 1, 1, 1, 304, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 485, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 363, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 425, 127, 1, 1, 61, 1, 61);
        List<Integer> values = Arrays.asList(19, 3639, 861681, 34622, 161984, 44342048, 69, 75324, 222811048, 3312861,
                3955, 50556, 422, 31596, 1, 1624019, 16576, 2806, 721, 509687, 584, 411819, 3320, 20924839, 3377, 5, 12,
                491, 3408, 175187264, 1, 436652529, 67400974, 51586, 1, 10969, 13, 46758, 8910928, 7468, 391179,
                1665445, 1100393, 87976, 13100, 14070, 2212, 4420225, 5597, 5, 196457, 995042260, 5662902, 225536, 17,
                1554, 1, 29352461, 137820, 1, 183, 17840, 7791288, 1701086, 52281081, 173, 298, 377268, 3165, 2982,
                29166574, 2439726, 5680, 569721, 1794, 7118, 3862461, 5, 132403, 1, 1652191, 71434, 14441758, 4698, 5,
                867, 37, 3, 1, 30, 65904, 108374, 21621, 9747486, 1202, 4643, 954632, 264461, 1, 4263);
        StreamingTombstoneHistogramBuilder b = new StreamingTombstoneHistogramBuilder(1, 5, 60);
        addAndVerify(b, keys, values);
    }

    @Test
    public void testQTFail2() throws Exception { // overflow in roundKey
        List<Integer> keys = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                2147483647, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1);
        List<Integer> values = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1);
        StreamingTombstoneHistogramBuilder b = new StreamingTombstoneHistogramBuilder(1, 5, 60);
        addAndVerify(b, keys, values);
    }

    @Test
    public void testQTFail3() throws Exception { // overflow on tryCell sum
        List<Integer> keys = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1);
        List<Integer> values = Arrays.asList(92854, 2253669, 305331, 2737696, 151, 1, 18, 718, 1661887, 6834, 30854003,
                4765669, 44561878, 25393, 11471938, 6996231, 134281, 6329, 158884, 176, 756, 579653, 889029700, 3188332,
                699, 73218, 77242, 19025, 348817, 285857394, 1764988, 32, 5836117, 172, 13838, 761, 12519, 2, 461, 113,
                1, 19, 111881, 1291585, 405931, 47, 27897, 3, 3662101, 384446, 9408, 13360093, 8867099, 106900391,
                1683167, 111970713, 204146, 239061, 1843, 160874, 1, 14369, 286, 607, 8073985, 263664, 1201198, 47021,
                2441, 324572, 1762822, 12812902, 103148019, 2148990, 128196, 27, 767, 1332739, 149165, 323752403, 37991,
                2391334, 1366658, 2076267, 6943, 125, 1, 118148783, 208199, 128813, 42174, 184652, 2765, 14204723,
                1653978, 440, 103260, 9552691, 90520, 671);
        StreamingTombstoneHistogramBuilder b = new StreamingTombstoneHistogramBuilder(1, 5, 60);
        addAndVerify(b, keys, values);
    }

    @Test
    public void testQTFail4() throws Exception { // oveflow on delta (1)
        List<Integer> keys = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 61, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1);
        List<Integer> values = Arrays.asList(108330698, 39, 1, 43274, 1124327, 551, 34605, 1157, 457101, 5273, 293, 1,
                26592, 36337, 2785776, 88, 23956, 13401, 3665, 1836, 357058, 849, 273461, 689165, 9011, 19639938, 27651,
                709, 7693, 1, 72929, 783995, 291461, 91277471, 32465281, 1, 21159, 654, 8295, 1348609, 69, 5277055,
                1039256, 36500, 256342954, 2463, 2479, 533144, 28, 2240, 165272, 456, 83697, 24309585, 1660, 615, 30,
                1342835, 26831, 797284518, 322320, 3327, 18, 1, 168766176, 13479370, 12123, 5233, 44, 1, 105398,
                1357578, 3368553, 461801, 478692238, 727, 26419001, 1, 88690, 168586, 10753, 28865, 74208, 462, 1583021,
                103574970, 1, 739412, 12493, 545, 869, 446107, 3583, 5698, 237, 495287, 360173, 1, 278568, 1160);
        StreamingTombstoneHistogramBuilder b = new StreamingTombstoneHistogramBuilder(1, 1, 60);
        addAndVerify(b, keys, values);
    }

    private void addAndVerify(StreamingTombstoneHistogramBuilder b, List<Integer> keys, List<Integer> values) {
        // testing keys:
        // due to the way the keys are merged, there's not much we can assert from the
        // test related to this part, unless we recreate the key merging mechanism
        // outside of the class as a reference and bench against it in the test
        // testing values:
        // - for a maxBins = 1 we can sum up the total and cap at Integer.MAX_VALUE, but
        // for more I can't think of a deterministic way to count values per bin due to
        // merging
        // - we can check that all visited pairs (key, value) are >0 meaning there's no
        // overflow, as we only supply positive values to the test

        long total = 0;
        for (int i = 0; i < keys.size(); i++) {
            int k = keys.get(i);
            int v = values.get(i);
            b.update(k, v);
            total += v;
        }

        TombstoneHistogram hist = b.build();

        Set<Integer> bins = new HashSet<>();
        AtomicLong s1 = new AtomicLong();
        hist.forEach((p, v) -> {
            assertTrue(p > 0);
            assertTrue(v > 0);
            s1.addAndGet(v);
            bins.add(p);
        });

        if (bins.size() == 1) {
            // due to int overflow the total will probably be capped at max int
            total = Math.min(total, Integer.MAX_VALUE);

            // total sum v1, only works if maxBinSize == 1
            assertEquals(total, s1.get());
            // total sum v2, only works if maxBinSize == 1
            long s2 = (long) hist.sum(Integer.MAX_VALUE);
            assertEquals(total, s2);
        }
    }

    private static Gen<StreamingTombstoneHistogramBuilder> genStreamingTombstoneHistogramBuilder() {
        Gen<Integer> bs = integers().from(1).upTo(1000);
        Gen<Integer> ss = integers().from(0).upTo(300000);
        return bs.zip(ss, (b, s) -> new StreamingTombstoneHistogramBuilder(b, s, 60));
    }

    @Test // inherited from CASSANDRA-14773
    public void testMathOverflowDuringRoundingOfLargeTimestamp() throws Exception
    {
        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 60);
        int[] samples = new int[] {
            Cell.MAX_DELETION_TIME - 2, // should not be missed as result of INTEGER math overflow on rounding
            59, 60, 119, 180, 181, 300, 400
        };
        for (int i = 0 ; i < samples.length ; i++)
            builder.update(samples[i]);

        TombstoneHistogram histogram = builder.build();

        assertEquals(asMap(histogram).size(), 5);
        assertEquals(asMap(histogram).get(180).intValue(), 4);
        assertEquals(asMap(histogram).get(240).intValue(), 1);
        assertEquals(asMap(histogram).get(300).intValue(), 1);
        assertEquals(asMap(histogram).get(420).intValue(), 1);
        assertEquals(asMap(histogram).get(Cell.MAX_DELETION_TIME).intValue(), 1);
    }

    @Test // inherited from CASSANDRA-14773
    public void testThatPointIsNotMissedBecauseOfRoundingToNoDeletionTime() throws Exception
    {
        int pointThatRoundedToNoDeletion = Cell.NO_DELETION_TIME - 2;
        assert pointThatRoundedToNoDeletion + pointThatRoundedToNoDeletion % 3 == Cell.NO_DELETION_TIME : "test data should be valid";

        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(5, 10, 3);
        builder.update(pointThatRoundedToNoDeletion);

        TombstoneHistogram histogram = builder.build();

        assertEquals(asMap(histogram).size(), 1);
        assertEquals(asMap(histogram).get(Cell.MAX_DELETION_TIME).intValue(), 1);
    }
}
