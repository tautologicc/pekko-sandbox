package com.epicgames.experiments.iot;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class IotSupervisor extends AbstractBehavior<Void> {
    public static Behavior<Void> create() {
        return Behaviors.setup(IotSupervisor::new);
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private IotSupervisor(ActorContext<Void> context) {
        super(context);
        context.getLog().info("IoT application started");
    }

    private Behavior<Void> onPostStop() {
        getContext().getLog().info("IoT application stopped");
        return this;
    }
}
