package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.algo.bitset.ReusableBitSet.INSTANCE;
import static net.openhft.chronicle.map.VanillaChronicleMap.*;

/**
 * Created by catst01 on 24/10/2018.
 */
public class MissSizedMapsTest {

    public static final AtomicInteger COUNT_1 = new AtomicInteger();
    public static final AtomicInteger COUNT_2 = new AtomicInteger();

    public static void main(String[] args) throws IOException, URISyntaxException {

        ChronicleMap<String, String> actual1 = createMapInstance();
        //  ChronicleMap<String, String> actual2 = createMapInstance();

        MissSizedMapsTest missSizedMapsTest = new MissSizedMapsTest();
        List<String> strings = missSizedMapsTest.sampleData();

        HashMap<String, String> expected1 = new HashMap<>();

        // HashMap<String, String> expected2 = new HashMap<>();

        for (String s : strings) {
            boolean wasOk1 = missSizedMapsTest.check(actual1, s, expected1, COUNT_1, "label-1");
            if (!wasOk1)
                break;

            //    boolean wasOk2 = missSizedMapsTest.check(actual2, s, expected2, COUNT_2, "label-2");
            //  if (!wasOk2)
            //    break;
        }

    }

    private static ChronicleMap<String, String> createMapInstance() throws IOException {
        return ChronicleMapBuilder.of(String.class, String.class)
                .averageKey("D-6.0149935894066442E18").averageValue("226|16533|4|1|1|testHarness").entries(150 << 10)
                .actualSegments(1)
                .createPersistedTo(File.createTempFile("chronicle", "cmap"));
    }

    AtomicBoolean w30 = new AtomicBoolean();

    String v;

    private boolean check(final ChronicleMap<String, String> actual,
                          String s,
                          Map<String, String> expected,
                          AtomicInteger count, String lable) {
        String[] split = s.split("&");
        String key = split[0];
        String value = split[1];

        if (key.equals("30|5235|4|1|1|testHarness")) {
            System.out.println(INSTANCE.isSet(124851));
            System.out.println("");
        }

        if ("27|31|4|1|1|testHarness".equals(key)) {
            System.out.println("");
        }

        if (count.get() == 52592) {

            System.out.println("BEFORE PUT \t- expected=" + expected.size() + ", actual= " + actual.size());
            System.out.println(INSTANCE.isSet(124851));

            int start = 4968576;
            long end = 4968754;
            System.out.println(bSeg.toHexString(start, end - start));
            Object old = expected.put(key, value);
            String old1 = actual.put(key, value);
         //   assert old1 == null;

            System.out.println("AFTER PUT \t- expected=" + expected.size() + ", actual= " + actual.size());
            printRow(actual, key);

      /*      if (expected.size() != actual.size()) {

                String k = actual.keySet()) .forEach(e -> expected.containsKey(e));

                System.out.println("expected keyset=" + expected.keySet());
                System.out.println("actual keyset=" + actual.keySet());
            }*/

        } else {
            Object old = expected.put(key, value);
            actual.put(key, value);

            if (w30.get() && !actual.containsKey("30|5235|4|1|1|testHarness")) {
                System.out.println("\n\n\n\n" + key);
            }
            printRow(actual, key);
        }

        if (key.equals("30|5235|4|1|1|testHarness"))
            w30.set(true);


        
  /*      if ("5|2311|4|1|1|testHarness".equals(key))
            System.out.println(expected.containsKey("9|19633|4|1|1|testHarness"));
*/
        count.incrementAndGet();

        if (expected.size() != actual.size()) {
            System.out.println("expected=" + expected.size());
            System.out.println("actual=" + actual.size());
            return false;
        }

        return true;
    }

    private void printRow(final ChronicleMap<String, String> actual, final String key) {
        String s1 = actual.get(key);
        System.out.println(lower + " - " + upper +
                "\t " +
                "key=" + key + ",\t value=" + s1 + "\t" + " " +
                "start=0x" + Long.toHexString(start) + ", end=0x" + Long.toHexString(end + 4));

        if (start >= 0x4bd0c0 && end <= 0x4bd0f6) {
            int start = 0x4bd0c0 - 64;
            long end = VanillaChronicleMap.end + 64;
            System.out.println(bSeg.toHexString(start, end - start));
            return;
        }

    }

    @NotNull
    private List<String> sampleData() throws URISyntaxException, IOException {
        URI uri = MissSizedMapsTest.class.getResource("/input.txt").toURI();

        return Files.readAllLines(Paths.get(uri));
    }

}

