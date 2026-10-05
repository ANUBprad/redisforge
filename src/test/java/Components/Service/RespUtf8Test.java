package Components.Service;

import Components.Infra.RespStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RESP is written down in bytes, so it has to be read back in bytes. A bulk string's header
 * states how many bytes its payload occupies, and a reader that measures that payload in
 * characters loses its place the moment a character takes more than one byte.
 *
 * <p>These tests frame and unframe values on their own, with no socket and no server, and
 * they do it at the byte level: one value at a time, several in a single read, split at
 * every position, and cut short.</p>
 */
class RespUtf8Test {

    /** every value the milestone asks for, plus one whose bytes dwarf its characters */
    private static final String ASCII = "hello";
    private static final String LATIN = "café";
    private static final String DEVANAGARI = "नमस्ते";
    private static final String JAPANESE = "こんにちは";
    private static final String EMOJI = "🌍";
    private static final String MIXED = "café नमस्ते 🌍";
    private static final String REORDERED = "नमस्ते 🌍 café";
    private static final String WIDE = "🌍🌍🌍🌍🌍";

    private static final List<String> VALUES = List.of(
            ASCII, LATIN, DEVANAGARI, JAPANESE, EMOJI, MIXED, REORDERED, WIDE);

    private final RespSerializer respSerializer = new RespSerializer();

    // ---------- encoding ----------

    @Test
    void aBulkStringHeaderCountsUtf8BytesRatherThanCharacters() {
        for (String value : VALUES) {
            int bytes = value.getBytes(StandardCharsets.UTF_8).length;

            assertEquals("$" + bytes + "\r\n" + value + "\r\n", respSerializer.serializeBulkString(value),
                    "the header of " + value + " does not state its byte length");
        }

        // the two that make the difference impossible to miss: one byte more than
        // characters, and four times as many bytes as characters
        assertEquals("$5\r\ncafé\r\n", respSerializer.serializeBulkString(LATIN));
        assertEquals("$20\r\n" + WIDE + "\r\n", respSerializer.serializeBulkString(WIDE));
        assertEquals(4, LATIN.length());
        // the emoji are surrogate pairs, so the JVM counts two characters each
        assertEquals(10, WIDE.length());
    }

    @Test
    void aCommandHeaderCountsUtf8BytesForEveryArgument() {
        String[] command = {"SET", "unicode:key", MIXED};
        int bytes = MIXED.getBytes(StandardCharsets.UTF_8).length;

        String frame = respSerializer.respArray(command);

        assertEquals(3, command.length);
        assertTrue(frame.contains("$" + bytes + "\r\n" + MIXED + "\r\n"),
                "the value's header does not state its byte length: " + frame);
        // the whole frame is nothing but headers and the payloads they count
        int expected = ("*3\r\n" + "$3\r\nSET\r\n" + "$11\r\nunicode:key\r\n"
                + "$" + bytes + "\r\n" + MIXED + "\r\n").getBytes(StandardCharsets.UTF_8).length;
        assertEquals(expected, frame.getBytes(StandardCharsets.UTF_8).length,
                "the framed command is not as long as its own headers and payloads");
        assertNotEquals(MIXED.length(), bytes, "this value no longer shows the difference");
    }

    @Test
    void aReplyIsAsLongAsItsHeaderSaysItIs() throws IOException {
        RespStream stream = new RespStream(respSerializer);
        for (String value : VALUES) {
            stream.append(respSerializer.serializeBulkString(value).getBytes(StandardCharsets.UTF_8), 0);
            // a reply is not a command, so nothing comes back out of the parser: what
            // matters is that the header describes the bytes that follow it exactly
        }
        assertEquals("$" + MIXED.getBytes(StandardCharsets.UTF_8).length,
                headerOf(respSerializer.serializeBulkString(MIXED)));
    }

    // ---------- decoding one frame ----------

    @Test
    void oneUtf8FrameComesBackExactly() {
        for (String value : VALUES) {
            byte[] frame = respSerializer.respArray(new String[]{"SET", "unicode:key", value}).getBytes(StandardCharsets.UTF_8);
            int length = respSerializer.frameLength(frame, 0, frame.length);

            assertEquals(frame.length, length, "the frame of " + value + " does not measure itself");
            List<String[]> commands = respSerializer.deseralize(frame);

            assertEquals(1, commands.size(), "a single frame produced " + commands.size() + " commands");
            assertArrayEquals(new String[]{"SET", "unicode:key", value}, commands.get(0));
        }
    }

    @Test
    void aFrameWithAnEmptyPayloadComesBackAsAnEmptyString() {
        byte[] frame = respSerializer.respArray(new String[]{"SET", "k", ""}).getBytes(StandardCharsets.UTF_8);

        assertTrue(frameContains(frame, "$0\r\n\r\n"), "an empty payload is not framed as a zero length one");
        List<String[]> commands = respSerializer.deseralize(frame);

        assertEquals(1, commands.size());
        assertArrayEquals(new String[]{"SET", "k", ""}, commands.get(0));
    }

    @Test
    void theCrlfBehindAMultibytePayloadIsConsumed() {
        // the payload ends in two and four byte characters, and the CRLF has to be read
        // after them rather than counted as part of them
        String value = "café 🌍";

        List<String[]> commands = respSerializer.deseralize(
                respSerializer.respArray(new String[]{"SET", "k", value}).getBytes(StandardCharsets.UTF_8));

        assertArrayEquals(new String[]{"SET", "k", value}, commands.get(0));
    }

    @Test
    void aPayloadMayHoldCrlfAndTheLookOfAFrame() {
        // a payload is a run of bytes: it can carry CRLF, and it can carry bytes that read
        // like the start of another frame, because the header says how long it is
        for (String value : List.of("a\r\nb", "*3\r\n$3\r\nSET\r\n$3\r\nx\r\n", "$5\r\n", "\r\n")) {
            byte[] frame = respSerializer.respArray(new String[]{"SET", "k", value}).getBytes(StandardCharsets.UTF_8);

            assertEquals(frame.length, respSerializer.frameLength(frame, 0, frame.length));
            assertArrayEquals(new String[]{"SET", "k", value},
                    respSerializer.deseralize(frame).get(0), "the payload did not survive intact");
        }
    }

    // ---------- several frames in one read ----------

    @Test
    void severalUtf8FramesInOneReadAllDecodeInOrder() {
        byte[] read = concat(
                respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}),
                respSerializer.respArray(new String[]{"PING"}),
                respSerializer.respArray(new String[]{"GET", "unicode:key"}));

        List<String[]> commands = respSerializer.deseralize(read);

        assertEquals(3, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, commands.get(0));
        assertArrayEquals(new String[]{"PING"}, commands.get(1));
        assertArrayEquals(new String[]{"GET", "unicode:key"}, commands.get(2));
    }

    @Test
    void aFrameMayBeginImmediatelyAfterAMultibytePayload() {
        // nothing separates the two frames but the payload's own CRLF, which is the place
        // a reader working in characters loses the boundary
        byte[] read = concat(
                respSerializer.respArray(new String[]{"SET", "unicode:key", REORDERED}),
                respSerializer.respArray(new String[]{"GET", "unicode:key"}));

        int first = respSerializer.respArray(new String[]{"SET", "unicode:key", REORDERED})
                .getBytes(StandardCharsets.UTF_8).length;
        assertEquals(first, respSerializer.frameLength(read, 0, read.length),
                "the first frame did not stop where its payload ended");
        assertEquals(read.length - first,
                respSerializer.frameLength(read, first, read.length),
                "the second frame did not start where the first one stopped");

        List<String[]> commands = respSerializer.deseralize(read);

        assertEquals(2, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:key", REORDERED}, commands.get(0));
        assertArrayEquals(new String[]{"GET", "unicode:key"}, commands.get(1));
    }

    @Test
    void aMultibyteFrameFollowedByAnotherOneIsHandedOutOneAtATime() throws IOException {
        RespStream stream = new RespStream(respSerializer);
        byte[] read = concat(
                respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}),
                respSerializer.respArray(new String[]{"PING"}));

        stream.append(read, read.length);
        List<String[]> commands = stream.drain();

        assertEquals(2, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, commands.get(0));
        assertArrayEquals(new String[]{"PING"}, commands.get(1));
        assertEquals(0, stream.drain().size(), "both frames were handed out twice");
    }

    // ---------- a frame split across reads ----------

    @Test
    void aUtf8CommandSplitAtEveryByteIsHeldUntilItIsWhole() throws IOException {
        byte[] frame = respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}).getBytes(StandardCharsets.UTF_8);

        for (int split = 1; split < frame.length; split++) {
            RespStream stream = new RespStream(respSerializer);
            stream.append(Arrays.copyOfRange(frame, 0, split), split);

            assertEquals(0, stream.drain().size(),
                    frame.length / 2 + 0 + " of the bytes were taken for a whole command");

            stream.append(Arrays.copyOfRange(frame, split, frame.length), frame.length - split);
            List<String[]> commands = stream.drain();

            assertEquals(1, commands.size(), "the command was not decoded once the rest arrived");
            assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, commands.get(0));
        }
    }

    @Test
    void twoUtf8FramesSplitAcrossReadsAreHeldIndependently() throws IOException {
        byte[] first = respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}).getBytes(StandardCharsets.UTF_8);
        byte[] second = respSerializer.respArray(new String[]{"SET", "unicode:other", REORDERED}).getBytes(StandardCharsets.UTF_8);
        RespStream stream = new RespStream(respSerializer);

        // the whole of the first, and a fragment of the second that stops inside its payload
        int cut = first.length + second.length / 2;
        byte[] read = concatBytes(first, second);
        stream.append(Arrays.copyOfRange(read, 0, cut), cut);

        List<String[]> commands = stream.drain();
        assertEquals(1, commands.size(), "a partial second frame was handed out");
        assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, commands.get(0));

        stream.append(Arrays.copyOfRange(read, cut, read.length), read.length - cut);
        commands = stream.drain();

        assertEquals(1, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:other", REORDERED}, commands.get(0));
    }

    @Test
    void aFrameWhoseBytesHaveNotAllArrivedIsNeverTakenForWhole() {
        byte[] frame = respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}).getBytes(StandardCharsets.UTF_8);

        for (int prefix = 0; prefix < frame.length; prefix++) {
            assertEquals(RespSerializer.INCOMPLETE_FRAME,
                    respSerializer.frameLength(frame, 0, prefix),
                    "a prefix of " + prefix + " bytes was taken for a whole command");
        }
        assertEquals(frame.length, respSerializer.frameLength(frame, 0, frame.length));
    }

    @Test
    void aTruncatedPayloadLeavesTheStreamWaitingRatherThanDecodingHalfAValue() throws IOException {
        byte[] frame = respSerializer.respArray(new String[]{"SET", "unicode:key", MIXED}).getBytes(StandardCharsets.UTF_8);
        RespStream stream = new RespStream(respSerializer);

        // cut three bytes short of the payload's CRLF
        int cut = frame.length - 3;
        stream.append(Arrays.copyOfRange(frame, 0, cut), cut);

        assertEquals(0, stream.drain().size(), "a truncated payload was decoded as a value");

        stream.append(Arrays.copyOfRange(frame, cut, frame.length), frame.length - cut);

        assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, stream.drain().get(0));
    }

    // ---------- what must not happen ----------

    @Test
    void theInputThatUsedToLoopForeverNowDecodes() {
        // a multibyte payload followed by more of the stream is what used to leave the
        // parser walking past the end of the payload and spinning there forever
        byte[] read = concat(
                respSerializer.respArray(new String[]{"SET", "unicode:key", WIDE}),
                respSerializer.respArray(new String[]{"SET", "unicode:other", MIXED}),
                respSerializer.respArray(new String[]{"PING"}));

        List<String[]> commands = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            List<String[]> decoded = new ArrayList<>();
            RespStream stream = new RespStream(respSerializer);
            for (int i = 0; i < read.length; i++) {
                stream.append(new byte[]{read[i]}, 1);
                decoded.addAll(stream.drain());
            }
            return decoded;
        });

        assertEquals(3, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:key", WIDE}, commands.get(0));
        assertArrayEquals(new String[]{"SET", "unicode:other", MIXED}, commands.get(1));
        assertArrayEquals(new String[]{"PING"}, commands.get(2));
    }

    @Test
    void malformedBytesAreRefusedWithoutHanging() {
        List<String> malformed = List.of(
                "hello\r\n",
                "*3\r\n?3\r\nSET\r\n",
                "*3\r\n$3\r\nSET\r\n$3\r\nk\r\n$-1\r\n",
                "*2\r\n$3\r\nGET\r\n$-1\r\n",
                "*1\r\n\r\n",
                "*1\r\n$99999999999\r\nx\r\n",
                "*1\r\n$3\r\nSE\r\n",
                "*1\r\n$3\r\nSET\n");

        for (String bytes : malformed) {
            byte[] data = bytes.getBytes(StandardCharsets.UTF_8);
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                int length = respSerializer.frameLength(data, 0, data.length);
                assertTrue(length < 0, "[" + printable(bytes) + "] was taken for a whole frame of " + length);
                RespStream stream = new RespStream(respSerializer);
                stream.append(data, data.length);
                // either the connection is refused or nothing comes out of it, but the
                // parser has to come back and say so
                try {
                    assertEquals(0, stream.drain().size());
                } catch (IOException expected) {
                    // no way to resynchronise, so the connection ends: that is allowed
                }
                return null;
            }, "the parser did not come back from [" + printable(bytes) + "]");
        }
    }

    @Test
    void theShortestValidFramesAreStillValid() {
        // the neighbouring cases of the malformed ones above, so refusing them is not
        // simply refusing everything short
        assertEquals(13, respSerializer.frameLength(
                "*1\r\n$3\r\nSET\r\n".getBytes(StandardCharsets.UTF_8), 0, 13));

        byte[] empty = "*0\r\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(4, respSerializer.frameLength(empty, 0, empty.length));
        List<String[]> commands = respSerializer.deseralize(empty);
        assertEquals(1, commands.size());
        assertEquals(0, commands.get(0).length, "an array of nothing holds no arguments");
    }

    @Test
    void aNullBulkStringIsAValidReplyButNeverAPartOfACommand() {
        // "$-1" is how RedisForge answers GET on a key that is not there: five bytes, and a
        // reader must take it as a whole reply
        assertEquals(5, "$-1\r\n".getBytes(StandardCharsets.UTF_8).length);

        byte[] asCommand = respSerializer.respArray(new String[]{"SET", "k", ""})
                .getBytes(StandardCharsets.UTF_8);
        String withNull = new String(asCommand, StandardCharsets.UTF_8).replace("$0\r\n\r\n", "$-1\r\n");
        byte[] data = withNull.getBytes(StandardCharsets.UTF_8);

        assertEquals(RespSerializer.MALFORMED_FRAME,
                respSerializer.frameLength(data, 0, data.length),
                "a negative bulk length was taken for a command");
        assertEquals(0, respSerializer.deseralize(data).size(),
                "a negative bulk length was decoded as an argument");
    }

    @Test
    void bytesThatAreNotUtf8DoNotDesyncTheStream() throws IOException {
        // the header counts bytes, so a payload that is not valid UTF-8 is still exactly
        // that many bytes long: it decodes to replacement characters and the frame behind
        // it is still found
        byte[] bad = {(byte) 0xFF, (byte) 0xFE, (byte) 0x80};
        byte[] ping = respSerializer.respArray(new String[]{"PING"}).getBytes(StandardCharsets.UTF_8);
        byte[] first = ("*2\r\n$3\r\nSET\r\n$" + bad.length + "\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] read = concatBytes(first, bad, "\r\n".getBytes(StandardCharsets.UTF_8), ping);

        assertEquals(read.length - ping.length, respSerializer.frameLength(read, 0, read.length),
                "a payload that is not UTF-8 changed where the first frame ends");

        RespStream stream = new RespStream(respSerializer);
        stream.append(read, read.length);
        List<String[]> commands = stream.drain();

        assertEquals(2, commands.size(), "a payload that is not UTF-8 lost the frame behind it");
        assertEquals(2, commands.get(0).length);
        assertEquals(3, commands.get(0)[1].length(), "the payload did not keep its byte count");
        assertArrayEquals(new String[]{"PING"}, commands.get(1));
    }

    @Test
    void nestedArraysStillHandOverOneCommandEach() {
        // the shape the stream has always accepted: an array of arrays, each of which is a
        // command of its own
        String two = "*2\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
                + "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\u0000";
        List<String[]> commands = respSerializer.deseralize(two.getBytes(StandardCharsets.UTF_8));

        assertEquals(2, commands.size());
        assertArrayEquals(new String[]{"set", "key", "value"}, commands.get(0));
        assertArrayEquals(new String[]{"set", "key", "value"}, commands.get(1));
    }

    @Test
    void aNestedArrayOfUtf8ValuesDecodesToo() {
        byte[] nested = ("*1\r\n*3\r\n$3\r\nSET\r\n$11\r\nunicode:key\r\n$"
                + MIXED.getBytes(StandardCharsets.UTF_8).length + "\r\n" + MIXED + "\r\n")
                .getBytes(StandardCharsets.UTF_8);

        List<String[]> commands = respSerializer.deseralize(nested);

        assertEquals(1, commands.size());
        assertArrayEquals(new String[]{"SET", "unicode:key", MIXED}, commands.get(0));
    }

    @Test
    void aFrameIsReadBackTheSameWayItWasWritten() {
        // the property everything else rests on: frame, write to a file, read back
        for (String value : VALUES) {
            byte[] written = respSerializer.respArray(new String[]{"SET", "unicode:key", value})
                    .getBytes(StandardCharsets.UTF_8);
            int length = respSerializer.frameLength(written, 0, written.length);

            assertArrayEquals(new String[]{"SET", "unicode:key", value},
                    respSerializer.deserializeFrame(written, 0, length));
        }
    }

    // ---------- helpers ----------

    private static void assertArrayEquals(String[] expected, String[] actual, String... because) {
        String why = because.length == 0 ? "" : String.join(" ", because) + ": ";
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual,
                why + "expected " + String.join(" ", expected) + " but got " + String.join(" ", actual));
    }

    private static byte[] concat(String... frames) {
        byte[][] parts = new byte[frames.length][];
        for (int i = 0; i < frames.length; i++) {
            parts[i] = frames[i].getBytes(StandardCharsets.UTF_8);
        }
        return concatBytes(parts);
    }

    private static byte[] concatBytes(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] all = new byte[length];
        int at = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, all, at, part.length);
            at += part.length;
        }
        return all;
    }

    private static boolean frameContains(byte[] frame, String text) {
        return new String(frame, StandardCharsets.UTF_8).contains(text);
    }

    private static String headerOf(String bulkString) {
        int end = bulkString.indexOf("\r\n");
        return bulkString.substring(0, end);
    }

    private static String printable(String bytes) {
        StringBuilder sb = new StringBuilder();
        for (char c : bytes.toCharArray()) {
            sb.append(c < ' ' ? '?' : c);
        }
        return sb.toString();
    }
}