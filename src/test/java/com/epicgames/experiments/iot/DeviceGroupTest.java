package com.epicgames.experiments.iot;

import org.apache.pekko.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

public class DeviceGroupTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testReplyToRegistrationRequests() {
        var groupId = TestRandom.uuid();
        var groupActor = testKit.spawn(DeviceGroup.create(groupId));
        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var recordProbe = testKit.createTestProbe(Device.TemperatureRecorded.class);

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .toList();
        var devices = deviceIds.stream()
                .map(deviceId -> {
                    var requestId = TestRandom.int64();
                    groupActor.tell(new DeviceManager.RequestTrackDevice(
                            requestId, groupId, deviceId, registeredProbe.getRef()));
                    return registeredProbe.receiveMessage();
                })
                .map(DeviceManager.DeviceRegistered::device)
                .distinct()
                .toList();
        assertEquals(deviceIds.size(), devices.size());

        for (var device : devices) {
            var requestId = TestRandom.int64();
            var temperature = TestRandom.float64();
            device.tell(new Device.RecordTemperature(requestId, temperature, recordProbe.getRef()));
            assertEquals(requestId, recordProbe.receiveMessage().requestId());
        }
    }

    @Test
    public void testIgnoreWrongRegistrationRequests() {
        var requestId = TestRandom.int64();
        var groupId = TestRandom.uuid();
        var deviceId = TestRandom.uuid();
        var wrongGroupId = TestRandom.uuid();

        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var groupActor = testKit.spawn(DeviceGroup.create(groupId));
        groupActor.tell(new DeviceManager.RequestTrackDevice(
                requestId, wrongGroupId, deviceId, registeredProbe.getRef()));
        registeredProbe.expectNoMessage();
    }

    @Test
    public void testReturnSameActorForSameDeviceId() {
        var groupId = TestRandom.uuid();
        var deviceId = TestRandom.uuid();

        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var groupActor = testKit.spawn(DeviceGroup.create(groupId));

        var devices = LongStream.generate(TestRandom::int64)
                .limit(5)
                .mapToObj(requestId -> {
                    groupActor.tell(new DeviceManager.RequestTrackDevice(
                            requestId, groupId, deviceId, registeredProbe.getRef()));
                    return registeredProbe.receiveMessage();
                })
                .map(DeviceManager.DeviceRegistered::device)
                .distinct()
                .toList();

        assertEquals(1, devices.size());
    }

    @Test
    public void testListActiveDevices() {
        var groupId = TestRandom.uuid();

        var registrationProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var groupActor = testKit.spawn(DeviceGroup.create(groupId));

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .collect(toSet());

        for (var deviceId : deviceIds) {
            var requestId = TestRandom.int64();
            groupActor.tell(new DeviceManager.RequestTrackDevice(
                    requestId, groupId, deviceId, registrationProbe.getRef()));
            registrationProbe.receiveMessage();
        }

        var deviceListProbe = testKit.createTestProbe(DeviceManager.ReplyAllDevices.class);

        var requestId = TestRandom.int64();
        groupActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
        var msg = deviceListProbe.receiveMessage();
        assertAll(
                () -> assertEquals(requestId, msg.requestId()),
                () -> assertEquals(deviceIds, msg.deviceIds()));
    }

    @Test
    public void testListActiveDevicesWhenOneShutsDown() {
        var groupId = TestRandom.uuid();

        var registeredProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered.class);
        var groupActor = testKit.spawn(DeviceGroup.create(groupId));

        var deviceIds = Stream.generate(TestRandom::uuid)
                .limit(5)
                .collect(toSet());

        ActorRef<Device.Command> deviceToShutDown = null;
        for (var deviceId : deviceIds) {
            var requestId = TestRandom.int64();
            groupActor.tell(new DeviceManager.RequestTrackDevice(
                    requestId, groupId, deviceId, registeredProbe.getRef()));
            DeviceManager.DeviceRegistered reply = registeredProbe.receiveMessage();
            if (deviceToShutDown == null) {
                deviceToShutDown = reply.device();
            }
        }
        assertNotNull(deviceToShutDown);

        var deviceListProbe = testKit.createTestProbe(DeviceManager.ReplyAllDevices.class);

        {
            var requestId = TestRandom.int64();
            groupActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
            DeviceManager.ReplyAllDevices reply = deviceListProbe.receiveMessage();
            assertAll(
                    () -> assertEquals(requestId, reply.requestId()),
                    () -> assertEquals(deviceIds, reply.deviceIds()));
        }

        deviceToShutDown.tell(Device.Passivate.INSTANCE);
        registeredProbe.expectTerminated(deviceToShutDown, registeredProbe.getRemainingOrDefault());

        registeredProbe.awaitAssert(() -> {
            var requestId = TestRandom.int64();
            groupActor.tell(new DeviceManager.RequestAllDevices(requestId, groupId, deviceListProbe.getRef()));
            DeviceManager.ReplyAllDevices reply = deviceListProbe.receiveMessage();
            assertAll(
                    () -> assertEquals(requestId, reply.requestId()),
                    () -> assertEquals(deviceIds.size() - 1, reply.deviceIds().size()));
            return null;
        });
    }
}