package Components.Service;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class RespSerializer {
    private static final Logger logger = Logger.getLogger(RespSerializer.class.getName());

    /** returned by {@link #frameLength} when the bytes so far do not hold a whole array yet */
    public static final int INCOMPLETE_FRAME = -1;
    /** returned by {@link #frameLength} when the bytes cannot be the start of an array */
    public static final int MALFORMED_FRAME = -2;

    /** the most a single length header may claim, which is the ceiling Redis itself uses */
    private static final int MAX_FRAME_LENGTH = 512 * 1024 * 1024;

    public String serializeBulkString(String s){
        // the header counts bytes on the wire, not characters in the JVM: "é" is two
        // bytes and "🌍" is four, and the reader takes the reader that many bytes before
        // it decodes anything
        int length = s.getBytes(StandardCharsets.UTF_8).length;
        String respHeader = "$"+length;
        String respBody = s;
        return respHeader + "\r\n" + respBody + "\r\n";
    }

    /**
     * Counts how many bytes the RESP array starting at {@code from} occupies.
     * An array is <code>*count\r\n</code> followed by that many
     * <code>$length\r\npayload\r\n</code> bulk strings.
     *
     * @return the length of the array in bytes, {@link #INCOMPLETE_FRAME} when the
     *         bytes received so far stop in the middle of one, or {@link #MALFORMED_FRAME}
     *         when they cannot be the start of an array at all
     */
    public int frameLength(byte[] data, int from, int to){
        if(from >= to){
            return INCOMPLETE_FRAME;
        }
        if(data[from] != '*'){
            return MALFORMED_FRAME;
        }

        int lineEnd = endOfLengthLine(data, from + 1, to);
        if(lineEnd < 0){
            return lineEnd;
        }
        int elements = parseLength(data, from + 1, lineEnd);
        if(elements < 0){
            return MALFORMED_FRAME;
        }

        int i = lineEnd + 2;
        for(int element = 0; element < elements; element++){
            if(i >= to){
                return INCOMPLETE_FRAME;
            }
            if(data[i] != '$'){
                return MALFORMED_FRAME;
            }

            lineEnd = endOfLengthLine(data, i + 1, to);
            if(lineEnd < 0){
                return lineEnd;
            }
            int length = parseLength(data, i + 1, lineEnd);
            if(length < 0){
                return MALFORMED_FRAME;
            }
            i = lineEnd + 2 + length;
            if(i + 1 >= to){
                return INCOMPLETE_FRAME;
            }
            if(data[i] != '\r' || data[i + 1] != '\n'){
                return MALFORMED_FRAME;
            }
            i += 2;
        }
        return i - from;
    }

    /**
     * Finds the '\r' closing a length header that starts at {@code from}, so the number
     * itself is data[from, result). Returns a negative {@link #frameLength} status.
     */
    private int endOfLengthLine(byte[] data, int from, int to){
        int i = from;
        while(i < to && data[i] != '\r'){
            if(data[i] < '0' || data[i] > '9'){
                return MALFORMED_FRAME;
            }
            i++;
        }
        if(i == to){
            return INCOMPLETE_FRAME;
        }
        if(i == from){
            return MALFORMED_FRAME;
        }
        if(i + 1 >= to){
            return INCOMPLETE_FRAME;
        }
        return i;
    }

    private int parseLength(byte[] data, int from, int to){
        // a header long enough to overflow an int is not a length anybody can send, and
        // letting it wrap would hand the rest of the parser a negative offset to walk
        long length = 0;
        for(int i = from; i < to; i++){
            length = length * 10 + (data[i] - '0');
            if(length > MAX_FRAME_LENGTH){
                return MALFORMED_FRAME;
            }
        }
        return (int) length;
    }

    public List<String[]> deseralize(byte[] command){
        return deseralize(command, 0, command.length);
    }

    /**
     * Decodes every RESP array held in data[from, to).
     *
     * <p>Everything is walked by byte. A bulk string's header says how many <em>bytes</em>
     * its payload takes, exactly those bytes are taken, the CRLF behind them is consumed,
     * and only then are they decoded. Measuring the payload in characters instead loses a
     * byte per multi byte character, which left the cursor somewhere inside the payload
     * and could not find its way out again.</p>
     *
     * <p>A nested array contributes each of its own elements as a command of its own,
     * which is how the command handler has always been handed them.</p>
     *
     * @return the commands held in that range, empty when the bytes are not a RESP array
     */
    public List<String[]> deseralize(byte[] command, int from, int to){
        List<String[]> commands = new ArrayList<>();
        int i = from;
        while(i < to){
            int read = readArray(command, i, to, commands);
            if(read < 0){
                logger.log(Level.WARNING, "stopped decoding at byte " + i + ": "
                        + (read == INCOMPLETE_FRAME ? "the array is not all here yet" : "these bytes are not a RESP array"));
                break;
            }
            i += read;
        }
        return commands;
    }

    /**
     * Reads one <code>*count\r\n</code> array of bulk strings starting at {@code from},
     * appending what it holds to {@code out}.
     *
     * @return how many bytes the array occupies, or a negative {@link #frameLength} status.
     *         Every branch either advances {@code i} or gives up, so no input can keep this
     *         walking in circles
     */
    private int readArray(byte[] data, int from, int to, List<String[]> out){
        if(from >= to){
            return INCOMPLETE_FRAME;
        }
        if(data[from] != '*'){
            return MALFORMED_FRAME;
        }
        int lineEnd = endOfLengthLine(data, from + 1, to);
        if(lineEnd < 0){
            return lineEnd;
        }
        int elements = parseLength(data, from + 1, lineEnd);
        if(elements < 0){
            return MALFORMED_FRAME;
        }

        int i = lineEnd + 2;
        List<String> flat = new ArrayList<>(elements);
        boolean nested = false;
        for(int element = 0; element < elements; element++){
            if(i >= to){
                return INCOMPLETE_FRAME;
            }
            if(data[i] == '*'){
                nested = true;
                int read = readArray(data, i, to, out);
                if(read < 0){
                    return read;
                }
                i += read;
                continue;
            }

            List<String> payload = new ArrayList<>(1);
            int read = readBulkString(data, i, to, payload);
            if(read < 0){
                return read;
            }
            flat.add(payload.get(0));
            i += read;
        }

        if(nested && !flat.isEmpty()){
            // an array holding both nested commands and bare bulk strings has no single
            // reading, and Redis refuses it as well
            return MALFORMED_FRAME;
        }
        if(!nested){
            out.add(flat.toArray(new String[0]));
        }
        return i - from;
    }

    /**
     * Reads one <code>$length\r\npayload\r\n</code> bulk string starting at {@code from}
     * and appends its decoded payload to {@code out}.
     *
     * @return how many bytes the bulk string occupies, or a negative {@link #frameLength}
     *         status
     */
    private int readBulkString(byte[] data, int from, int to, List<String> out){
        if(from >= to){
            return INCOMPLETE_FRAME;
        }
        if(data[from] != '$'){
            return MALFORMED_FRAME;
        }
        if(from + 1 < to && data[from + 1] == '-'){
            // "$-1" is the null bulk string: a reply RedisForge writes, and never part of a
            // command, so it is refused here instead of being taken for a length
            return MALFORMED_FRAME;
        }

        int lineEnd = endOfLengthLine(data, from + 1, to);
        if(lineEnd < 0){
            return lineEnd;
        }
        int length = parseLength(data, from + 1, lineEnd);
        if(length < 0){
            return MALFORMED_FRAME;
        }

        int payloadStart = lineEnd + 2;
        if(payloadStart + length > to){
            // the payload is still arriving, or was cut short: there is nothing to decode
            return INCOMPLETE_FRAME;
        }
        int terminator = endOfPayload(data, payloadStart + length, to);
        if(terminator < 0){
            return terminator;
        }

        out.add(new String(data, payloadStart, length, StandardCharsets.UTF_8));
        return payloadStart - from + length + terminator;
    }

    /**
     * Checks what closes a bulk string whose payload ends at {@code payloadEnd}. The end
     * of the data counts as a close, so a stream that stops on a payload still counts as
     * the whole array it belongs to, and the NUL this project has always used to close a
     * frame is accepted too.
     *
     * @return how many bytes the closer takes, or a negative {@link #frameLength} status
     */
    private int endOfPayload(byte[] data, int payloadEnd, int to){
        if(payloadEnd == to){
            return 0;
        }
        if(data[payloadEnd] == '\r'){
            if(payloadEnd + 1 >= to){
                return INCOMPLETE_FRAME;
            }
            return data[payloadEnd + 1] == '\n' ? 2 : MALFORMED_FRAME;
        }
        if(data[payloadEnd] == 0){
            return 1;
        }
        return MALFORMED_FRAME;
    }

    public String respInteger(int i){
        StringBuilder sb = new StringBuilder();
        sb.append(":");
        sb.append(i);
        sb.append("\r\n");
        return sb.toString();
    }

    public String respArray(String[] command) {
        List<String> res = new ArrayList<>();
        int len = command.length;
        res.add("*"+len);
        for(String s: command){
            // the header counts bytes on the wire, not chars in the JVM: a value like
            // "é" is two bytes in UTF-8, and framing it as one byte would shift
            // everything that follows it
            len = s.getBytes(StandardCharsets.UTF_8).length;
            res.add("$"+len);
            res.add(s);
        }
        return String.join("\r\n",res)+"\r\n";
    }

    public String respArray(List<String> command) {
        List<String> res = new ArrayList<>();
        int len = command.size();
        res.add("*"+len+"\r\n");
        res.addAll(command);
        return String.join("",res);
    }

/**
     * Decodes one whole frame by byte count, which is how a RESP frame is written down.
     *
     * <p>The same reader serves the network and the append only file, so a value that was
     * framed from the store comes back the way it went in whatever its bytes look
     * like.</p>
     *
     * @param data the bytes, from a '*' through the end of the array
     * @param to the end of this frame, which {@link #frameLength(byte[], int, int)} agreed on
     * @return the array's elements, empty when the bytes do not hold one whole array
     */
    public String[] deserializeFrame(byte[] data, int from, int to){
        List<String[]> commands = new ArrayList<>(1);
        int read = readArray(data, from, to, commands);
        if(read < 0 || commands.isEmpty()){
            return new String[0];
        }
return commands.get(0);
    }

    public String[] parseArray(String[] parts) {
        String len = parts[0];
        int length = Integer.parseInt(len);

        String _command[] = new String[length];

        _command[0] = parts[2];

        int idx = 1;
        for(int i=4; i < parts.length; i+=2){
            _command[idx++] = parts[i];
        }
        return _command;
    }
}
