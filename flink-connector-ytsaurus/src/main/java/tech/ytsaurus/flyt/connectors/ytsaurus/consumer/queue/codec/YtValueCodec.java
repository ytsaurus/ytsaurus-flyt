package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

@FunctionalInterface
public interface YtValueCodec {
    byte[] decompress(byte[] compressed);
}
