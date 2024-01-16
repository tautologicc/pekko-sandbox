package com.epicgames.experiments.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

// NOTE(mmm): Putting this annotation in the entrypoint class to force SLF4J to
// start up before Pekko does, so it stops whining about "intercepted logs
// being replayed".
@Slf4j
public class Supervision {
    public static void main(String[] args) {
        ActorSystem<String> supervisionTest = ActorSystem.create(SupervisingActor.create(), "supervisionTest");
        supervisionTest.tell("failChild");
        supervisionTest.tell("stop");
    }
}

class SupervisingActor extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(SupervisingActor::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals("failChild", this::onFailChild)
                .onMessageEquals("stop", Behaviors::stopped)
                .build();
    }

    private final ActorRef<String> child;

    private SupervisingActor(ActorContext<String> context) {
        super(context);
        this.child = context.spawn(
                Behaviors.supervise(SupervisedActor.create())
                        .onFailure(SupervisorStrategy.restart()),
                "supervised-actor");
    }

    private Behavior<String> onFailChild() {
        child.tell("fail");
        return this;
    }
}

class SupervisedActor extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(SupervisedActor::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals("fail", this::fail)
                .onSignal(PreRestart.class, signal -> onPreRestart())
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private SupervisedActor(ActorContext<String> context) {
        super(context);
        System.out.println("supervised actor started");
    }

    private Behavior<String> fail() {
        System.out.println("supervised actor fails now");
        throw new RuntimeException("failed");
    }

    private Behavior<String> onPreRestart() {
        System.out.println("supervised actor restarting");
        return this;
    }

    private Behavior<String> onPostStop() {
        System.out.println("supervised actor stopped");
        return this;
    }


}

