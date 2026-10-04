package Components.Infra;

import Components.Service.RespSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Holds the bytes of one connection between the TCP socket and the RESP parser.
 * A socket read is not a message: it can carry half a command, several commands, or the
 * tail of one command followed by the head of the next. Whatever cannot be decoded yet
 * stays here and is decoded once the rest of it arrives.
 */
public class RespStream {
    private static final Logger logger = Logger.getLogger(RespStream.class.getName());

    private final RespSerializer respSerializer;
    private byte[] buffered = new byte[0];
    private int filled = 0;

    public RespStream(RespSerializer respSerializer){
        this.respSerializer = respSerializer;
    }

    /** Adds the {@code length} bytes a socket read actually produced. */
    public void append(byte[] chunk, int length){
        if(length <= 0){
            return;
        }
        if(filled + length > buffered.length){
            buffered = Arrays.copyOf(buffered, filled + length);
        }
        System.arraycopy(chunk, 0, buffered, filled, length);
        filled += length;
    }

    /**
     * Decodes every complete array currently buffered and drops them from the buffer.
     *
     * @throws IOException if the buffered bytes are not RESP at all, which leaves the
     *         caller no way to resynchronise and so has to end the connection
     */
    public List<String[]> drain() throws IOException {
        List<String[]> commands = new ArrayList<>();
        int consumed = 0;
        while(consumed < filled){
            int frameLength = respSerializer.frameLength(buffered, consumed, filled);
            if(frameLength == RespSerializer.INCOMPLETE_FRAME){
                break;
            }
            if(frameLength == RespSerializer.MALFORMED_FRAME){
                logger.log(Level.WARNING, "closing connection, buffered bytes are not a RESP array");
                throw new IOException("malformed RESP frame at offset " + consumed);
            }
            commands.addAll(respSerializer.deseralize(buffered, consumed, consumed + frameLength));
            consumed += frameLength;
        }

        if(consumed > 0){
            System.arraycopy(buffered, consumed, buffered, 0, filled - consumed);
            filled -= consumed;
        }
        return commands;
    }
}