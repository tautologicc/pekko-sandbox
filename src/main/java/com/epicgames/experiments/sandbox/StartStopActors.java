package com.epicgames.experiments.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

// NOTE(mmm): Putting this annotation in the entrypoint class to force SLF4J to
// start up before Pekko does, so it stops whining about "intercepted logs
// being replayed".
@Slf4j
public class StartStopActors {
    public static void main(String[] args) {
        ActorSystem<String> startStopSystem = ActorSystem.create(StartStopActor1.create(), "start-stop");
        startStopSystem.tell("stop");
    }
}

class StartStopActor1 extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(StartStopActor1::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals("stop", Behaviors::stopped)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private StartStopActor1(ActorContext<String> context) {
        super(context);
        System.out.println("first started");

        context.spawn(StartStopActor2.create(), "second");
    }

    private Behavior<String> onPostStop() {
        System.out.println("first stopped");
        return this;
    }
}

class StartStopActor2 extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(StartStopActor2::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private StartStopActor2(ActorContext<String> context) {
        super(context);
        System.out.println("second started");
    }

    private Behavior<String> onPostStop() {
        System.out.println("second stopped");
        return this;
    }
}
