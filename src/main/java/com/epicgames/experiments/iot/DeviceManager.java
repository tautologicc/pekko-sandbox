package com.epicgames.experiments.iot;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DeviceManager extends AbstractBehavior<DeviceManager.Command> {
    public interface Command {
    }

    public record RequestTrackDevice(
            long requestId,
            String groupId,
            String deviceId,
            ActorRef<DeviceRegistered> replyTo
    ) implements DeviceManager.Command, DeviceGroup.Command {
    }

    public record DeviceRegistered(
            long requestId,
            ActorRef<Device.Command> device
    ) {
    }

    public record RequestAllDevices(
            long requestId,
            String groupId,
            ActorRef<ReplyAllDevices> replyTo
    ) implements DeviceManager.Command, DeviceGroup.Command {
    }

    public record ReplyAllDevices(
            long requestId,
            Set<String> deviceIds
    ) {
    }

    public record RequestAllTemperatures(
            long requestId,
            String groupId,
            ActorRef<ReplyAllTemperatures> replyTo
    ) implements DeviceManager.Command, DeviceGroup.Command {
    }

    public record ReplyAllTemperatures(
            long requestId,
            Map<String, TemperatureReading> temperatures
    ) {
    }

    public interface TemperatureReading {
    }

    public record Temperature(
            double value
    ) implements TemperatureReading {
    }

    public enum TemperatureNotAvailable implements TemperatureReading {
        INSTANCE;
    }

    public enum DeviceNotAvailable implements TemperatureReading {
        INSTANCE;
    }

    public enum DeviceTimedOut implements TemperatureReading {
        INSTANCE;
    }

    private record DeviceGroupTerminated(
            String groupId
    ) implements DeviceManager.Command {
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(DeviceManager::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RequestTrackDevice.class, this::onTrackDevice)
                .onMessage(RequestAllDevices.class, this::onAllDevices)
                .onMessage(RequestAllTemperatures.class, this::onAllTemperatures)
                .onMessage(DeviceGroupTerminated.class, this::onTerminated)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private final Map<String, ActorRef<DeviceGroup.Command>> groupActorById = new HashMap<>();

    private DeviceManager(ActorContext<Command> context) {
        super(context);

        context.getLog().info("Device manager started");
    }

    private Behavior<Command> onTrackDevice(RequestTrackDevice msg) {
        groupActorById.computeIfAbsent(msg.groupId, this::createGroup).tell(msg);
        return this;
    }

    private ActorRef<DeviceGroup.Command> createGroup(String groupId) {
        var groupActor = getContext().spawn(DeviceGroup.create(groupId), "group-" + groupId);

        getContext().getLog().atInfo()
                .addKeyValue("group", groupId)
                .log("Device group actor created");

        return groupActor;
    }

    private Behavior<Command> onAllDevices(RequestAllDevices msg) {
        var groupActor = groupActorById.get(msg.groupId);
        if (groupActor != null) {
            groupActor.tell(msg);
        } else {
            msg.replyTo.tell(new ReplyAllDevices(msg.requestId, Set.of()));
        }

        return this;
    }

    private Behavior<Command> onAllTemperatures(RequestAllTemperatures msg) {
        var groupActor = groupActorById.get(msg.groupId);
        if (groupActor != null) {
            groupActor.tell(msg);
        } else {
            msg.replyTo.tell(new ReplyAllTemperatures(msg.requestId, Map.of()));
        }

        return this;
    }

    private Behavior<Command> onTerminated(DeviceGroupTerminated msg) {
        if (groupActorById.keySet().remove(msg.groupId)) {
            getContext().getLog().atInfo()
                    .addKeyValue("group", msg.groupId)
                    .log("Device group actor has been terminated");
        }

        return this;
    }

    private Behavior<Command> onPostStop() {
        getContext().getLog().info("Device manager stopped");

        return this;
    }
}

