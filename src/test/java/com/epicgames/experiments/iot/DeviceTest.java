package com.epicgames.experiments.iot;

import org.apache.pekko.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeviceTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testReplyWithEmptyReadingIfNoTemperatureIsKnown() {
        var groupId = TestRandom.uuid();
        var deviceId = TestRandom.uuid();
        var deviceActor = testKit.spawn(Device.create(groupId, deviceId));

        var requestId = TestRandom.int64();
        var queryProbe = testKit.createTestProbe(Device.Temperature.class);
        deviceActor.tell(new Device.ReadTemperature(requestId, queryProbe.getRef()));
        var tempReply = queryProbe.receiveMessage();
        assertAll(
                () -> assertEquals(requestId, tempReply.requestId()),
                () -> assertEquals(OptionalDouble.empty(), tempReply.value()));
    }

    @Test
    public void testReplyWithLatestTemperatureReading() {
        final var groupId = TestRandom.uuid();
        final var deviceId = TestRandom.uuid();
        var deviceActor = testKit.spawn(Device.create(groupId, deviceId));

        var recordProbe = testKit.createTestProbe(Device.TemperatureRecorded.class);
        var readProbe = testKit.createTestProbe(Device.Temperature.class);

        // NOTE(mmm): Generate {n} test cases.
        for (long i = 0; i < 10; i++) {
            var temperature = TestRandom.float64();
            {
                var requestId = TestRandom.int64();
                deviceActor.tell(new Device.RecordTemperature(requestId, temperature, recordProbe.getRef()));
                assertEquals(requestId, recordProbe.receiveMessage().requestId());
            }
            {
                final var requestId = TestRandom.int64();
                deviceActor.tell(new Device.ReadTemperature(requestId, readProbe.getRef()));
                var reply = readProbe.receiveMessage();
                assertAll(
                        () -> assertEquals(requestId, reply.requestId()),
                        () -> assertEquals(deviceId, reply.deviceId()),
                        () -> assertEquals(OptionalDouble.of(temperature), reply.value()));
            }
        }
    }
}