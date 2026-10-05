package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.YTsaurusClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ConsumerYtQueuePullerFactoryTest {
    @Test
    void createsDistinctNonOwningPullersAndOwnsSharedClient() throws Exception {
        YTsaurusClient client = mock(YTsaurusClient.class);
        ConsumerYtQueuePullerFactory factory = new ConsumerYtQueuePullerFactory(
                client,
                "//home/test/consumer",
                "//home/test/queue");

        YtQueuePuller first = factory.get();
        YtQueuePuller second = factory.get();

        assertThat(first).isInstanceOf(ConsumerYtQueuePuller.class);
        assertThat(second).isInstanceOf(ConsumerYtQueuePuller.class).isNotSameAs(first);
        first.close();
        second.close();
        verifyNoInteractions(client);

        factory.close();
        factory.close();
        verify(client, times(1)).close();
    }

    @Test
    void rejectsPullerCreationAfterClose() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        ConsumerYtQueuePullerFactory factory = new ConsumerYtQueuePullerFactory(
                client,
                "//home/test/consumer",
                "//home/test/queue");
        factory.close();

        assertThatThrownBy(factory::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Queue puller factory is closed");
    }
}
