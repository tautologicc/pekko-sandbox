package com.epicgames.experiments.iot;

import org.apache.pekko.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeviceGroupQueryTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testReturnTemperatureValueForWorkingDevices() {
        // NOTE(mmm): This test also exercises the ability of the system to return temperatures from multiple devices.
        //  We use this fact to simplify testing edge cases.

        var tempsProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .collect(toSet());
        var deviceTemperatures = deviceIds.stream()
                .collect(toMap(identity(), id -> new DeviceManager.Temperature(TestRandom.float64())));

        var deviceProbeById = deviceIds.stream()
                .collect(toMap(identity(), id -> testKit.createTestProbe(Device.Command.class)));
        var deviceActorById = deviceProbeById.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().getRef()));

        var requestId = TestRandom.int64();
        var queryActor = testKit.spawn(DeviceGroupQuery.create(
                requestId, deviceActorById, tempsProbe.getRef(), Duration.ofSeconds(3)));
        deviceProbeById.values().forEach(
                probe -> probe.expectMessageClass(Device.ReadTemperature.class));

        deviceTemperatures.forEach((id, temp) ->
                queryActor.tell(new DeviceGroupQuery.ReplyTemperature(
                        new Device.Temperature(requestId, id, OptionalDouble.of(temp.value())))));
        DeviceManager.ReplyAllTemperatures reply = tempsProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());
        assertEquals(deviceTemperatures, reply.temperatures());
    }

    @Test
    public void testReturnTemperatureNotAvailableForDevicesWithNoReadings() {
        var requestId = TestRandom.int64();
        var deviceId = TestRandom.uuid();
        var deviceProbe = testKit.createTestProbe(Device.Command.class);
        var tempsProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);
        var deviceActorById = Map.of(deviceId, deviceProbe.getRef());

        var queryActor = testKit.spawn(DeviceGroupQuery.create(
                requestId, deviceActorById, tempsProbe.getRef(), Duration.ofSeconds(3)));

        assertEquals(requestId, deviceProbe.expectMessageClass(Device.ReadTemperature.class).requestId());

        var deviceTemp = OptionalDouble.empty();
        queryActor.tell(
                new DeviceGroupQuery.ReplyTemperature(
                        new Device.Temperature(requestId, deviceId, deviceTemp)));

        DeviceManager.ReplyAllTemperatures reply = tempsProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());

        var expectedTemperatures = Map.of(deviceId, DeviceManager.TemperatureNotAvailable.INSTANCE);
        assertEquals(expectedTemperatures, reply.temperatures());
    }

    @Test
    public void testReturnDeviceNotAvailableIfDeviceStopsBeforeAnswering() {
        var requestId = TestRandom.int64();
        var deviceId = TestRandom.uuid();
        var deviceProbe = testKit.createTestProbe(Device.Command.class);
        var tempsProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);
        var deviceActorById = Map.of(deviceId, deviceProbe.getRef());

        testKit.spawn(DeviceGroupQuery.create(
                requestId, deviceActorById, tempsProbe.getRef(), Duration.ofSeconds(3)));

        assertEquals(requestId, deviceProbe.expectMessageClass(Device.ReadTemperature.class).requestId());

        deviceProbe.stop();

        DeviceManager.ReplyAllTemperatures reply = tempsProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());

        var expectedTemperatures = Map.of(deviceId, DeviceManager.DeviceNotAvailable.INSTANCE);
        assertEquals(expectedTemperatures, reply.temperatures());
    }

    @Test
    public void testReturnTemperatureReadingEvenIfDeviceStopsAfterAnswering() {
        var requestId = TestRandom.int64();
        var deviceId = TestRandom.uuid();
        var deviceProbe = testKit.createTestProbe(Device.Command.class);
        var tempsProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);
        var deviceActorById = Map.of(deviceId, deviceProbe.getRef());

        var queryActor = testKit.spawn(DeviceGroupQuery.create(
                requestId, deviceActorById, tempsProbe.getRef(), Duration.ofSeconds(3)));

        assertEquals(requestId, deviceProbe.expectMessageClass(Device.ReadTemperature.class).requestId());

        var deviceTemp = OptionalDouble.of(TestRandom.float64());
        queryActor.tell(
                new DeviceGroupQuery.ReplyTemperature(
                        new Device.Temperature(requestId, deviceId, deviceTemp)));

        deviceProbe.stop();

        DeviceManager.ReplyAllTemperatures reply = tempsProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());

        var expectedTemperatures = Map.of(deviceId, new DeviceManager.Temperature(deviceTemp.getAsDouble()));
        assertEquals(expectedTemperatures, reply.temperatures());
    }

    @Test
    public void testReturnDeviceTimedOutIfDeviceDoesNotAnswerInTime() {
        var requestId = TestRandom.int64();
        var deviceId = TestRandom.uuid();
        var deviceProbe = testKit.createTestProbe(Device.Command.class);
        var tempsProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);
        var deviceActorById = Map.of(deviceId, deviceProbe.getRef());

        var queryActor = testKit.spawn(DeviceGroupQuery.create(
                requestId, deviceActorById, tempsProbe.getRef(), Duration.ofMillis(200)));

        assertEquals(requestId, deviceProbe.expectMessageClass(Device.ReadTemperature.class).requestId());

        // NOTE(mmm): No reply from the device.

        DeviceManager.ReplyAllTemperatures reply = tempsProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());

        var expectedTemperatures = Map.of(deviceId, DeviceManager.DeviceTimedOut.INSTANCE);
        assertEquals(expectedTemperatures, reply.temperatures());
    }
}