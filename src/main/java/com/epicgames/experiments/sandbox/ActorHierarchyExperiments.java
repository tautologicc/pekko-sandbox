package com.epicgames.experiments.sandbox;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class ActorHierarchyExperiments {
    public static void main(String[] args) {
        ActorSystem<String> testSystem = ActorSystem.create(Main.create(), "testSystem");
        testSystem.tell("start");
    }
}

class Main extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(Main::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals("start", this::onStart)
                .build();
    }

    private Main(ActorContext<String> context)  {
        super(context);
    }

    private Behavior<String> onStart() {
        ActorRef<String> firstRef = getContext().spawn(PrintMyActorRefActor.create(), "first");
        System.out.println("first: " + firstRef);

        firstRef.tell("printRef");

        return Behaviors.same();
    }

}

class PrintMyActorRefActor extends AbstractBehavior<String> {
    public static Behavior<String> create() {
        return Behaviors.setup(PrintMyActorRefActor::new);
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals("printRef", this::onPrintRef)
                .build();
    }

    private PrintMyActorRefActor(ActorContext<String> context) {
        super(context);
    }

    private Behavior<String> onPrintRef() {
        ActorRef<Object> secondRef = getContext().spawn(Behaviors.empty(), "second-actor");
        System.out.println("second: " + secondRef);

        return Behaviors.same();
    }
}
