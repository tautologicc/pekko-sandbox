package com.epicgames.experiments.iot;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.*;

public class DeviceGroupQuery extends AbstractBehavior<DeviceGroupQuery.Command> {
    public interface Command {
    }

    record ReplyTemperature(
            Device.Temperature response
    ) implements Command {
    }

    record DeviceTerminated(
            String deviceId
    ) implements Command {
    }

    enum CollectionTimeout implements Command {
        INSTANCE;
    }

    public static Behavior<Command> create(
            long requestId,
            Map<String, ActorRef<Device.Command>> deviceActorById,
            ActorRef<DeviceManager.ReplyAllTemperatures> replyTo,
            Duration timeout
    ) {
        return Behaviors.setup(context ->
                Behaviors.withTimers(timers ->
                        new DeviceGroupQuery(context, timers, requestId, deviceActorById, replyTo, timeout)));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReplyTemperature.class, this::onRespondTemperature)
                .onMessage(DeviceTerminated.class, this::onDeviceTerminated)
                .onMessage(CollectionTimeout.class, this::onCollectionTimeout)
                .build();
    }

    private final long requestId;
    private final Map<String, DeviceManager.TemperatureReading> repliesSoFar = new HashMap<>();
    private final Set<String> stillWaiting;
    private final ActorRef<DeviceManager.ReplyAllTemperatures> replyTo;

    private DeviceGroupQuery(
            ActorContext<Command> context,
            TimerScheduler<Command> timers,
            long requestId,
            Map<String, ActorRef<Device.Command>> deviceActorById,
            ActorRef<DeviceManager.ReplyAllTemperatures> replyTo,
            Duration timeout
    ) {
        super(context);

        this.requestId = requestId;
        this.stillWaiting = new HashSet<>(deviceActorById.keySet());
        this.replyTo = replyTo;

        timers.startSingleTimer(CollectionTimeout.INSTANCE, timeout);

        var msg = new Device.ReadTemperature(requestId, context.messageAdapter(
                Device.Temperature.class, ReplyTemperature::new));
        for (var entry : deviceActorById.entrySet()) {
            var deviceId = entry.getKey();
            var deviceActor = entry.getValue();
            context.watchWith(deviceActor, new DeviceTerminated(deviceId));
            deviceActor.tell(msg);
        }
    }

    private Behavior<Command> onRespondTemperature(ReplyTemperature msg) {
        DeviceManager.TemperatureReading reading = DeviceManager.TemperatureNotAvailable.INSTANCE;
        OptionalDouble value = msg.response.value();
        if (value.isPresent()) {
            reading = new DeviceManager.Temperature(value.getAsDouble());
        }

        var deviceId = msg.response.deviceId();
        repliesSoFar.put(deviceId, reading);
        stillWaiting.remove(deviceId);

        return respondWhenAllCollected();
    }

    private Behavior<Command> onDeviceTerminated(DeviceTerminated msg) {
        if (stillWaiting.remove(msg.deviceId)) {
            repliesSoFar.put(msg.deviceId, DeviceManager.DeviceNotAvailable.INSTANCE);
        }

        return respondWhenAllCollected();
    }

    private Behavior<Command> onCollectionTimeout(CollectionTimeout msg) {
        for (var deviceId : stillWaiting) {
            repliesSoFar.put(deviceId, DeviceManager.DeviceTimedOut.INSTANCE);
        }
        stillWaiting.clear();

        return respondWhenAllCollected();
    }

    private Behavior<Command> respondWhenAllCollected() {
        if (!stillWaiting.isEmpty()) {
            return this;
        }

        replyTo.tell(new DeviceManager.ReplyAllTemperatures(requestId, repliesSoFar));
        return Behaviors.stopped();
    }
}
