package com.epicgames.experiments.iot;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorSystem;
import org.slf4j.LoggerFactory;

@Slf4j
public class IotMain {
    static {
        // NOTE(mmm): Calling the logger factory here to force SLF4J to start
        // up before Pekko does, so it stops whining about "intercepted logs
        // being replayed".
        LoggerFactory.getLogger("");
    }

    public static void main(String[] args) {
        ActorSystem.create(IotSupervisor.create(), "iot-system");
    }
}
