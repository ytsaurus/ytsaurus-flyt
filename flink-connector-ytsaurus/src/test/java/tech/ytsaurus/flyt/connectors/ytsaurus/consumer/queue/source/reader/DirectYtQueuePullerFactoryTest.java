package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.YTsaurusClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DirectYtQueuePullerFactoryTest {
    private static final String QUEUE_PATH = "//home/test/queue";

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "//home/test/consumer")
    void getCreatesDistinctNonOwningPullers(String consumerPath) throws Exception {
        YTsaurusClient client = mock(YTsaurusClient.class);
        DirectYtQueuePullerFactory factory = new DirectYtQueuePullerFactory(client, QUEUE_PATH, consumerPath);

        YtQueuePuller first = factory.get();
        YtQueuePuller second = factory.get();

        assertThat(first).isInstanceOf(DirectYtQueuePuller.class);
        assertThat(second).isInstanceOf(DirectYtQueuePuller.class);
        assertThat(first).isNotSameAs(second);

        first.close();
        second.close();
        verifyNoInteractions(client);
        factory.close();
        verify(client).close();
    }

    @Test
    void closeIsIdempotentAndClosesSharedClientOnce() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        DirectYtQueuePullerFactory factory = new DirectYtQueuePullerFactory(client, QUEUE_PATH);

        factory.close();
        factory.close();

        verify(client, times(1)).close();
    }

    @Test
    void getIsRejectedAfterClose() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        DirectYtQueuePullerFactory factory = new DirectYtQueuePullerFactory(client, QUEUE_PATH);
        factory.close();

        assertThatThrownBy(factory::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Queue puller factory is closed");
        verify(client).close();
    }

    @Test
    void adapterDelegatesToOrdinarySupplierAndHasNoOpClose() throws Exception {
        YtQueuePuller puller = mock(YtQueuePuller.class);
        Supplier<YtQueuePuller> supplier = () -> puller;
        YtQueuePullerFactory factory = YtQueuePullerFactory.fromSupplier(supplier);

        assertThat(factory.get()).isSameAs(puller);
        factory.close();

        verifyNoInteractions(puller);
    }
}
