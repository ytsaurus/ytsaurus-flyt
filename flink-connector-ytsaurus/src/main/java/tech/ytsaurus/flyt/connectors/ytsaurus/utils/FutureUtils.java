package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FutureUtils {

    public static <T> T getNow(CompletableFuture<T> future) {
        if (!future.isDone()) {
            throw new IllegalStateException("future is not completed yet");
        }
        return future.getNow(null);
    }


    public static <T> CompletableFuture<List<T>> allOf(List<? extends CompletableFuture<T>> futures) {
        CompletableFuture<T>[] futuresArray = futures.toArray(new CompletableFuture[0]);
        CompletableFuture<Void> allOfFuture = CompletableFuture.allOf(futuresArray);
        return allOfFuture.thenApply(v -> Stream.of(futuresArray)
                .map(FutureUtils::getNow)
                .collect(Collectors.toList())
        );
    }
}
