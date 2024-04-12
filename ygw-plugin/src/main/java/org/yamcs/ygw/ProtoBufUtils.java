package org.yamcs.ygw;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import us.hebi.quickbuf.InvalidProtocolBufferException;
import us.hebi.quickbuf.ProtoMessage;

public class ProtoBufUtils {
    public static <T extends ProtoMessage<T>> T fromByteBuf(ByteBuf buf, T msg)
            throws InvalidProtocolBufferException {
        final byte[] array;
        final int offset;
        final int length = buf.readableBytes();
        if (buf.hasArray()) {
            array = buf.array();
            offset = buf.arrayOffset() + buf.readerIndex();
        } else {
            array = ByteBufUtil.getBytes(buf, buf.readerIndex(), length, false);
            offset = 0;
        }

        return (T) ProtoMessage.mergeFrom(msg, array, offset, length);
    }
}
