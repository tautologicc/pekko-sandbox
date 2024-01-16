package com.epicgames.experiments.iot;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TestRandom {
    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static int int32() {
        return ThreadLocalRandom.current().nextInt();
    }

    public static long int64() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static double float64() {
        return ThreadLocalRandom.current().nextDouble();
    }
}
