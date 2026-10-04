package Components.Service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespSerializerTest {
    private final RespSerializer respSerializer = new RespSerializer();
    @Test
    public void testDeserializePing(){
        String ping = "*1\r\n$4\r\nPING\r\n";
        List<String[]> commands = respSerializer.deseralize(ping.getBytes(StandardCharsets.UTF_8));

        for(String[] s : commands){
            System.out.println("=====================================================================================");
            for(String ss: s){
                System.out.print(ss+" ");
            }
        }
        assertEquals(1, commands.size());
        assertEquals(1, commands.get(0).length);
        assertEquals("PING", commands.get(0)[0]);
    }
    @Test
    public void testMultipleCommands(){
        //"\u0000"
        String multipleCommands = "*2\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\u0000";
        List<String[]> commands = respSerializer.deseralize(multipleCommands.getBytes(StandardCharsets.UTF_8));
        System.out.println(commands.size());
        for(String[] s : commands){
            System.out.println("=====================================================================================");
            for(String ss: s){
                System.out.print(ss+" ");
            }
        }
        assertEquals(2, commands.size());
        assertEquals(3, commands.get(0).length);
        assertEquals(3, commands.get(1).length);

        assertEquals("set", commands.get(0)[0]);
        assertEquals("key", commands.get(0)[1]);
        assertEquals("value", commands.get(0)[2]);

        assertEquals("set", commands.get(1)[0]);
        assertEquals("key", commands.get(1)[1]);
        assertEquals("value", commands.get(1)[2]);
    }

    @Test
    public void testFrameLengthIsIncompleteUntilTheWholeArrayArrives(){
        String frame = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        byte[] bytes = frame.getBytes(StandardCharsets.UTF_8);

        // however the stream is cut up, no prefix may look like a whole frame
        for(int prefix = 0; prefix < bytes.length; prefix++){
            assertEquals(RespSerializer.INCOMPLETE_FRAME, respSerializer.frameLength(bytes, 0, prefix),
                    "a frame of " + prefix + " bytes was taken for a whole array");
        }
        assertEquals(bytes.length, respSerializer.frameLength(bytes, 0, bytes.length));
    }

    @Test
    public void testFrameLengthStopsAtTheFirstOfSeveralFrames(){
        String twoFrames = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        byte[] bytes = twoFrames.getBytes(StandardCharsets.UTF_8);
        int first = "*1\r\n$4\r\nPING\r\n".length();

        assertEquals(first, respSerializer.frameLength(bytes, 0, bytes.length));
        assertEquals(first, respSerializer.frameLength(bytes, first, bytes.length));
    }

    @Test
    public void testFrameLengthRejectsBytesThatAreNotResp(){
        assertEquals(RespSerializer.MALFORMED_FRAME,
                respSerializer.frameLength("hello\r\n".getBytes(StandardCharsets.UTF_8), 0, 7));
        assertEquals(RespSerializer.MALFORMED_FRAME,
                respSerializer.frameLength("*3\r\n?3\r\nSET\r\n".getBytes(StandardCharsets.UTF_8), 0, 16));
    }
}