package com.epicgames.experiments.iot;

import org.apache.pekko.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DeviceManagerTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testListActiveDevices() {
        var managerActor = testKit.spawn(DeviceManager.create());
        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var deviceListProbe = testKit.createTestProbe(DeviceManager.ReplyAllDevices.class);

        var groupId = TestRandom.uuid();

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .collect(toSet());

        for (var deviceId : deviceIds) {
            var requestId = TestRandom.int64();
            managerActor.tell(new DeviceManager.RequestTrackDevice(
                    requestId, groupId, deviceId, registeredProbe.getRef()));
            registeredProbe.receiveMessage();
        }

        var requestId = TestRandom.int64();
        managerActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
        DeviceManager.ReplyAllDevices reply = deviceListProbe.receiveMessage();
        assertEquals(requestId, reply.requestId());
        assertEquals(deviceIds, reply.deviceIds());
    }

    @Test
    public void testListActiveDevicesWhenOneShutsDown() {
        var managerActor = testKit.spawn(DeviceManager.create());
        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var deviceListProbe = testKit.createTestProbe(DeviceManager.ReplyAllDevices.class);

        var groupId = TestRandom.uuid();

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .collect(toSet());

        ActorRef<Device.Command> deviceToShutDown = null;
        for (var deviceId : deviceIds) {
            var requestId = TestRandom.int64();
            managerActor.tell(new DeviceManager.RequestTrackDevice(
                    requestId, groupId, deviceId, registeredProbe.getRef()));
            DeviceManager.DeviceRegistered reply = registeredProbe.receiveMessage();
            if (deviceToShutDown == null) {
                deviceToShutDown = reply.device();
            }
        }
        assertNotNull(deviceToShutDown);

        {
            var requestId = TestRandom.int64();
            managerActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
            DeviceManager.ReplyAllDevices reply = deviceListProbe.receiveMessage();
            assertEquals(requestId, reply.requestId());
            assertEquals(deviceIds, reply.deviceIds());
        }

        deviceToShutDown.tell(Device.Passivate.INSTANCE);
        registeredProbe.expectTerminated(deviceToShutDown, registeredProbe.getRemainingOrDefault());

        registeredProbe.awaitAssert(() -> {
            long requestId = 1L;
            managerActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
            DeviceManager.ReplyAllDevices reply = deviceListProbe.receiveMessage();
            assertEquals(requestId, reply.requestId());
            assertEquals(deviceIds.size() - 1, reply.deviceIds().size());
            return null;
        });
    }

    @Test
    public void testCollectTemperaturesFromAllActiveDevices() {
        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var groupActor = testKit.spawn(DeviceGroup.create("group"));

        groupActor.tell(new DeviceManager.RequestTrackDevice(0L, "group", "device1", registeredProbe.getRef()));
        var deviceActor1 = registeredProbe.receiveMessage().device();

        groupActor.tell(new DeviceManager.RequestTrackDevice(0L, "group", "device2", registeredProbe.getRef()));
        var deviceActor2 = registeredProbe.receiveMessage().device();

        groupActor.tell(new DeviceManager.RequestTrackDevice(0L, "group", "device3", registeredProbe.getRef()));
        registeredProbe.receiveMessage();

        var recordProbe = testKit.createTestProbe(Device.TemperatureRecorded.class);
        deviceActor1.tell(new Device.RecordTemperature(0L, 1, recordProbe.getRef()));
        assertEquals(0L, recordProbe.receiveMessage().requestId());
        deviceActor2.tell(new Device.RecordTemperature(1L, 2, recordProbe.getRef()));
        assertEquals(1L, recordProbe.receiveMessage().requestId());
        // NOTE(mmm): No temperature for device 3.

        var allTempProbe = testKit.createTestProbe(DeviceManager.ReplyAllTemperatures.class);
        groupActor.tell(new DeviceManager.RequestAllTemperatures(0L, "group", allTempProbe.getRef()));
        var response = allTempProbe.receiveMessage();
        assertEquals(0L, response.requestId());

        var expectedTemperatures = Map.of(
                "device1", new DeviceManager.Temperature(1),
                "device2", new DeviceManager.Temperature(2),
                "device3", DeviceManager.TemperatureNotAvailable.INSTANCE);
        assertEquals(expectedTemperatures, response.temperatures());
    }
}