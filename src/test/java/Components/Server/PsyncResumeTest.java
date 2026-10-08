package Components.Server;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Service.ResponseDto;
import Config.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a master answers a replica that asks to carry its stream on.
 *
 * <p>Two answers, and which one a replica gets has to follow from facts alone: a replica
 * naming this master's replication id and a position whose bytes are still held is carried
 * on from there with the bytes it missed written out behind the answer, and everything else
 * - an id this master does not have, a position ahead of the stream, one that has fallen
 * out of the window, one that is not a number at all, or a replica that has never synced -
 * is answered with the whole stream, which is always safe because the replica takes on
 * whatever position this master's stream has reached.</p>
 *
 * <p>These tests drive the handler directly, with the bytes it writes captured, so the
 * decision is watched rather than inferred: both answers have to appear on the socket
 * itself - a resume with the bytes it missed behind it, a full resync with the dataset
 * behind it - and neither may be handed back to the caller, because a write made in the
 * gap between an answer and its bytes would take the stream apart.</p>
 */
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PsyncResumeTest {

    private static final String REPLID = "0123456789abcdef0123456789abcdef";

    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RespSerializer respSerializer;

    private ByteArrayOutputStream written;
    private Client client;

    @BeforeEach
    void aStreamOfItsOwn() {
        written = new ByteArrayOutputStream();
        client = new Client(new Socket(), new ByteArrayInputStream(new byte[0]), written, 1);
        // every test starts from an empty stream at a position of its own, and the window may
        // only be resized while it holds nothing - which is exactly where this leaves it
        redisConfig.setMasterReplId(REPLID);
        redisConfig.setMasterReplOffset(0L);
        redisConfig.setReplBacklogSize(RedisConfig.DEFAULT_REPL_BACKLOG_SIZE);
    }

    @AfterEach
    void letGoOfTheConnection() {
        connectionPool.removeClient(client);
        connectionPool.removeSlave(client);
        client.close();
    }

    @Test
    void aPositionTheBacklogStillHoldsIsResumedWithTheBytesTheReplicaMissed() throws Exception {
        long afterFirst = record("SET", "resume:before", "one");
        long afterSecond = record("SET", "resume:gap", "two");

        ResponseDto reply = psync(REPLID, Long.toString(afterFirst));

        assertNull(reply, "a resume has to be written out rather than handed back to be written later");
        assertArrayEquals(
                join(continueLine(), frame("SET", "resume:gap", "two")),
                written.toByteArray(),
                "the replica was not carried on from its own position with exactly the bytes it missed");
        assertEquals(afterSecond, redisConfig.getMasterReplOffset().longValue(), "PSYNC moved the stream");
    }

    @Test
    void aPositionAtTheEndOfTheStreamIsResumedWithNothingToCarry() throws Exception {
        record("SET", "resume:one", "one");
        long end = redisConfig.getMasterReplOffset();

        ResponseDto reply = psync(REPLID, Long.toString(end));

        assertNull(reply, "a replica that stopped on the newest byte was handed the dataset again");
        assertArrayEquals(continueLine(), written.toByteArray(),
                "a resume with nothing to carry should say so and send nothing else");
        assertEquals(end, redisConfig.getMasterReplOffset().longValue(), "PSYNC moved the stream");
    }

    @Test
    void aReplicationIdThatIsNotThisMastersIsAnsweredWithTheWholeStream() throws Exception {
        record("SET", "a", "1");

        assertNull(psync("ffffffffffffffffffffffffffffffff", "0"),
                "a full resync has to be written out, not handed back to be written later");

        assertWholeStream(redisConfig.getMasterReplOffset().longValue());
    }

    @Test
    void aPositionAheadOfTheStreamIsAnsweredWithTheWholeStream() throws Exception {
        long end = record("SET", "a", "1");

        assertNull(psync(REPLID, Long.toString(end + 100)),
                "a position ahead of the stream has to be written out, not handed back");

        assertWholeStream(end);
    }

    @Test
    void aPositionThatHasFallenOutOfTheBacklogIsAnsweredWithTheWholeStream() throws Exception {
        redisConfig.setReplBacklogSize(8);
        long end = record("SET", "a", "1");
        // twenty five bytes into an eight byte window: everything the replica would ask for
        // that is older than the window's own tail has already been let go of
        assertEquals(end - 8, redisConfig.getReplicationBacklog().startOffset(),
                "the window did not slide to the tail of the frame it was given");

        assertNull(psync(REPLID, Long.toString(end - 9)),
                "a position the window has let go of has to be written out, not handed back");
        assertWholeStream(end);
        assertNull(psync(REPLID, "0"),
                "a position behind the window has to be written out, not handed back");
        assertWholeStream(end);
    }

    @Test
    void theOldestPositionTheBacklogStillHoldsIsResumed() throws Exception {
        redisConfig.setReplBacklogSize(8);
        long end = record("SET", "a", "1");
        byte[] frame = frame("SET", "a", "1");
        byte[] tail = new byte[8];
        System.arraycopy(frame, frame.length - 8, tail, 0, 8);

        ResponseDto reply = psync(REPLID, Long.toString(end - 8));

        assertNull(reply, "the position the window still holds was answered with the whole stream");
        assertArrayEquals(join(continueLine(), tail), written.toByteArray(),
                "the resume did not carry exactly the window's own tail");

        written.reset();
        assertNull(psync(REPLID, Long.toString(end - 9)),
                "a position the window has let go of has to be written out, not handed back");
        assertWholeStream(end);
    }

    @Test
    void resizingTheWindowAfterTheStreamHasMovedKeepsBothNamingTheSamePosition() throws Exception {
        long end = record("SET", "aligned", "one");

        // the size is moved once the stream has already travelled: the window is emptied
        // onto the position it is standing on, and it has to come out of the change still
        // naming that same position, or every byte recorded afterwards would be kept under
        // a name the offset does not know
        redisConfig.setMasterReplOffset(end);
        redisConfig.setReplBacklogSize(64);
        assertEquals(end, redisConfig.getReplicationBacklog().startOffset(),
                "the window started again at 0 and no longer agrees with the offset");

        long after = record("SET", "aligned", "two");
        assertNull(psync(REPLID, Long.toString(end)),
                "the stream was answered with the whole thing after the window was resized");
        assertArrayEquals(join(continueLine(), frame("SET", "aligned", "two")), written.toByteArray(),
                "the resume did not carry the bytes written after the window was resized");

        written.reset();
        assertNull(psync(REPLID, Long.toString(after)), "the newest position was not resumed");
        assertArrayEquals(continueLine(), written.toByteArray(), "the resume should have carried nothing");
    }

    @Test
    void aPositionThatIsNotANumberIsAnsweredWithTheWholeStream() throws Exception {
        long end = record("SET", "a", "1");

        assertNull(psync(REPLID, "not-a-position"),
                "a position this replica cannot have counted to has to be written out");
        assertWholeStream(end);
        assertNull(psync(REPLID, "12.5"),
                "a position this replica cannot have counted to has to be written out");
        assertWholeStream(end);
        assertNull(psync(REPLID, ""),
                "a position this replica cannot have counted to has to be written out");
        assertWholeStream(end);
    }

    @Test
    void aQuestionMarkAsksForTheWholeStreamWhateverPositionItCarries() throws Exception {
        long end = record("SET", "a", "1");

        assertNull(psync("?", "0"), "a replica that has never synced has to be written the stream");
        assertWholeStream(end);
        assertNull(psync("?", Long.toString(end)),
                "a replica that has never synced has to be written the stream");
        assertWholeStream(end);
    }

    @Test
    void psyncWithoutAPositionIsRefusedAsAWrongNumberOfArguments() throws Exception {
        ResponseDto reply = commandHandler.psync(new String[]{"PSYNC", REPLID}, client);

        assertNotNull(reply, "an incomplete request was answered as if it could be resumed from");
        assertEquals("-ERR wrong number of arguments for 'psync' command\r\n", reply.response);
        assertNull(reply.data);

        ResponseDto noArguments = commandHandler.psync(new String[]{"PSYNC"}, client);
        assertEquals("-ERR wrong number of arguments for 'psync' command\r\n", noArguments.response);
        assertEquals(0, written.size());
    }

    @Test
    void aReplicasStreamStartsOnBothAnswers() throws Exception {
        // registered the way a replica registers itself, before it asks for anything
        Slave resumed = register();
        assertNull(psync(REPLID, "0"), "an empty window at the stream's own position should resume");
        assertTrue(resumed.isReady(), "a resumed replica was not made ready to be sent writes");
        assertArrayEquals(continueLine(), written.toByteArray(), "the resume said nothing about carrying on");
        written.reset();

        connectionPool.removeSlave(client);
        Slave replaced = register();
        assertNull(psync("?", "-1"), "a replica with no stream should be given the whole thing");
        assertWholeStream(redisConfig.getMasterReplOffset().longValue());
        assertTrue(replaced.isReady(), "a replica given the whole stream was not made ready to be sent writes");
    }

    @Test
    void framesComeBackByteForByteIncludingMultibyteAndLargeOnes() throws Exception {
        String big = "\u0939\u093F\u0928\u094D\u0926\u0940 \uD83C\uDF0D h\u00E9llo ".repeat(8_000);
        record("SET", "resume:large", big);
        long after = record("SET", "resume:after", "small");

        ResponseDto reply = psync(REPLID, "0");

        assertNull(reply, "a frame the window holds was answered with the whole stream");
        byte[] expected = join(join(continueLine(), frame("SET", "resume:large", big)),
                frame("SET", "resume:after", "small"));
        assertTrue(expected.length > 90_000, "the test's own frame is not large: " + expected.length);
        assertArrayEquals(expected, written.toByteArray(),
                "a large multibyte frame did not come back through the window byte for byte");
        assertEquals(after, redisConfig.getMasterReplOffset().longValue());
    }

    // ------------------------------------------------------------- helpers

    private ResponseDto psync(String replicaId, String position) throws IOException {
        return commandHandler.psync(new String[]{"PSYNC", replicaId, position}, client);
    }

    private long record(String... command) {
        return redisConfig.recordReplicatedBytes(frame(command));
    }

    private Slave register() {
        Slave slave = new Slave(client, 16_380);
        connectionPool.addSlave(slave);
        return slave;
    }

    private byte[] continueLine() {
        return ("+CONTINUE " + REPLID + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Checks that the socket carries the whole stream from the position the master has
     * reached: the resync line with that position, and a dataset of the length it declares.
     * The captured bytes are cleared afterwards, so a test may hand over the stream again.
     */
    private void assertWholeStream(long expectedOffset) {
        byte[] answer = written.toByteArray();

        int statusEnd = 0;
        while (statusEnd + 1 < answer.length && answer[statusEnd] != '\r') {
            statusEnd++;
        }
        assertTrue(statusEnd > 0 && answer[statusEnd + 1] == '\n',
                "nothing was written to the socket for a full resync");
        assertEquals("+FULLRESYNC " + REPLID + " " + expectedOffset,
                new String(answer, 0, statusEnd, StandardCharsets.UTF_8),
                "the replica was not moved to the position the stream has reached");

        int headerStart = statusEnd + 2;
        int headerEnd = headerStart;
        while (headerEnd < answer.length && answer[headerEnd] != '\r') {
            headerEnd++;
        }
        assertTrue(headerEnd > headerStart + 1 && headerEnd + 1 < answer.length
                        && answer[headerStart] == '$' && answer[headerEnd + 1] == '\n',
                "the dataset did not start with its length");
        int length = Integer.parseInt(
                new String(answer, headerStart + 1, headerEnd - headerStart - 1,
                        StandardCharsets.US_ASCII));
        assertEquals(headerEnd + 2 + length, answer.length,
                "the dataset was not sent whole behind the length it declares");

        written.reset();
    }

    private static byte[] frame(String... parts) {
        StringBuilder resp = new StringBuilder();
        resp.append('*').append(parts.length).append("\r\n");
        for (String part : parts) {
            resp.append('$').append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n")
                    .append(part).append("\r\n");
        }
        return resp.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] join(byte[] first, byte[] second) {
        byte[] joined = new byte[first.length + second.length];
        System.arraycopy(first, 0, joined, 0, first.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }
}
