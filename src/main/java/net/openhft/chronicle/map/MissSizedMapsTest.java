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
                .createPersistedTo(File.createTempFile("chronicle", "cmap"));
    }

    AtomicBoolean writtenKey = new AtomicBoolean();

    private boolean check(final ChronicleMap<String, String> actual,
                          String s,
                          Map<String, String> expected,
                          AtomicInteger count, String lable) {
        String[] split = s.split("&");
        String key = split[0];
        Object old = expected.put(key, split[1]);
        //System.out.println("old=" + old);
        //if (old == null)

        if (count.get() == 85124) {
            boolean before = actual.containsKey("9|19633|4|1|1|testHarness");
            actual.put(key, split[1].substring(0, split[1].length() - 4));
            boolean after = actual.containsKey("9|19633|4|1|1|testHarness");

            System.out.println("before=" + before);
            System.out.println("after=" + after);

        }

        if (key.equals("9|19633|4|1|1|testHarness")) {
            writtenKey.set(true);
        }

        if (count.get() == 99504)
            actual.put(key, split[1]);
        else
            actual.put(key, split[1]);
        count.incrementAndGet();

        if (!actual.containsKey(key))
            System.out.println("key");

        if (actual.size() != expected.size()) {
            System.out.println(lable + " count=" + count.get());
            return false;
        }

        if (writtenKey.get()) {
            if (!actual.containsKey("9|19633|4|1|1|testHarness"))
                System.out.println("it got coruppted here");
        }

        return true;
    }

    @NotNull
    private List<String> sampleData() throws URISyntaxException, IOException {
        URI uri = MissSizedMapsTest.class.getResource("/input.txt").toURI();

        return Files.readAllLines(Paths.get(uri));
    }

}
