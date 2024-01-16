package com.epicgames.experiments.iot;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class DeviceGroup extends AbstractBehavior<DeviceGroup.Command> {
    public interface Command {
    }

    private record DeviceTerminated(
            ActorRef<Device.Command> device,
            String groupId,
            String deviceId
    ) implements Command {
    }

    public static Behavior<Command> create(String groupId) {
        return Behaviors.setup(context -> new DeviceGroup(context, groupId));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                // TODO(mmm): Doesn't this message handler need a guard clause??
                //  Check it after cleaning up the DeviceManager class.
                .onMessage(DeviceManager.RequestTrackDevice.class, this::onTrackDevice)
                .onMessage(
                        DeviceManager.RequestAllDevices.class,
                        msg -> groupId.equals(msg.groupId()),
                        this::onAllDevices)
                .onMessage(
                        DeviceManager.RequestAllTemperatures.class,
                        msg -> groupId.equals(msg.groupId()),
                        this::onAllTemperatures)
                .onMessage(DeviceTerminated.class, this::onDeviceTerminated)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private final String groupId;
    private final Map<String, ActorRef<Device.Command>> deviceActorById = new HashMap<>();

    private DeviceGroup(ActorContext<Command> context, String groupId) {
        super(context);
        this.groupId = groupId;

        context.getLog().atInfo()
                .setMessage("Device group started")
                .addKeyValue("groupId", groupId)
                .log();
    }

    private Behavior<Command> onTrackDevice(DeviceManager.RequestTrackDevice msg) {
        if (groupId.equals(msg.groupId())) {
            var device = deviceActorById.computeIfAbsent(msg.deviceId(), this::createDevice);
            msg.replyTo().tell(new DeviceManager.DeviceRegistered(msg.requestId(), device));

        } else {
            getContext().getLog().atError()
                    .setMessage("DeviceGroup actor asked to act on behalf of another group, ignoring request")
                    .addKeyValue("requestGroupId", msg.groupId())
                    .addKeyValue("groupId", groupId)
                    .log();
        }

        return this;
    }

    private ActorRef<Device.Command> createDevice(String deviceId) {
        var deviceActor = getContext().spawn(Device.create(groupId, deviceId), "device-" + deviceId);

        getContext().watchWith(deviceActor, new DeviceTerminated(deviceActor, groupId, deviceId));

        return deviceActor;
    }

    private Behavior<Command> onAllDevices(DeviceManager.RequestAllDevices msg) {
        // NOTE(mmm): HashMap is mutable in Java, so we take a "snapshot" (copy)
        // of the underlying key set at this point in time.
        var deviceIds = new HashSet<>(deviceActorById.keySet());
        msg.replyTo().tell(new DeviceManager.ReplyAllDevices(msg.requestId(), deviceIds));

        return this;
    }

    private Behavior<Command> onAllTemperatures(DeviceManager.RequestAllTemperatures msg) {
        // NOTE(mmm): Same thing as above.
        var deviceActorById_ = new HashMap<>(this.deviceActorById);
        getContext().spawnAnonymous(
                DeviceGroupQuery.create(
                        msg.requestId(),
                        deviceActorById_,
                        msg.replyTo(),
                        Duration.ofSeconds(3)));

        return this;
    }

    private Behavior<Command> onDeviceTerminated(DeviceTerminated msg) {
        if (deviceActorById.keySet().remove(msg.deviceId)) {
            getContext().getLog().atInfo()
                    .setMessage("Device actor has been terminated")
                    .addKeyValue("groupId", msg.groupId)
                    .addKeyValue("deviceId", msg.deviceId)
                    .log();
        }

        return this;
    }

    private Behavior<Command> onPostStop() {
        getContext().getLog().atInfo()
                .setMessage("Device group stopped")
                .addKeyValue("groupId", groupId)
                .log();

        return this;
    }

}
