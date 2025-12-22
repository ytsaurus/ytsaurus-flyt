package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class ThreadUtils {

    public static void sleepWithJitter(double sleepSeconds) throws InterruptedException {
        double timeWithJitter = sleepSeconds * 0.9 + Math.random() * sleepSeconds * 0.2;
        log.info("Sleeping {} seconds", timeWithJitter);
        Thread.sleep((long) (timeWithJitter * 1000));
    }

}
