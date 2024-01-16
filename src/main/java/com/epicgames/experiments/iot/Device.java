package com.epicgames.experiments.iot;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.OptionalDouble;

public class Device extends AbstractBehavior<Device.Command> {
    public interface Command {
    }

    public record RecordTemperature(
            long requestId,
            double value,
            ActorRef<TemperatureRecorded> replyTo
    ) implements Command {
    }

    public record TemperatureRecorded(
            long requestId
    ) {
    }

    public record ReadTemperature(
            long requestId,
            ActorRef<Temperature> replyTo
    ) implements Command {
    }

    public record Temperature(
            long requestId,
            String deviceId,
            OptionalDouble value
    ) {
    }

    public enum Passivate implements Command {
        INSTANCE;
    }

    public static Behavior<Command> create(String groupId, String deviceId) {
        return Behaviors.setup(context -> new Device(context, groupId, deviceId));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RecordTemperature.class, this::onRecordTemperature)
                .onMessage(ReadTemperature.class, this::onReadTemperature)
                .onMessage(Passivate.class, msg -> Behaviors.stopped())
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private final String groupId;
    private final String deviceId;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private OptionalDouble lastTemperatureReading = OptionalDouble.empty();

    private Device(ActorContext<Command> context, String groupId, String deviceId) {
        super(context);
        this.groupId = groupId;
        this.deviceId = deviceId;

        context.getLog().atInfo()
                .setMessage("Device started")
                .addKeyValue("groupId", groupId)
                .addKeyValue("deviceId", deviceId)
                .log();
    }

    private Behavior<Command> onRecordTemperature(RecordTemperature msg) {
        lastTemperatureReading = OptionalDouble.of(msg.value);

        getContext().getLog().atInfo()
                .setMessage("Temperature reading recorded")
                .addKeyValue("requestId", msg.requestId)
                .addKeyValue("value", msg.value)
                .log();

        msg.replyTo.tell(new TemperatureRecorded(msg.requestId));

        return this;
    }

    private Behavior<Command> onReadTemperature(ReadTemperature msg) {
        msg.replyTo.tell(new Temperature(msg.requestId, deviceId, lastTemperatureReading));

        return this;
    }

    private Behavior<Command> onPostStop() {
        getContext().getLog().atInfo()
                .setMessage("Device stopped")
                .addKeyValue("groupId", groupId)
                .addKeyValue("deviceId", deviceId)
                .log();

        return this;
    }
}
