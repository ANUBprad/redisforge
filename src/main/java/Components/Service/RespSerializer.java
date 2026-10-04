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

    public String serializeBulkString(String s){
        int length = s.length();
        String respHeader = "$"+length;
        String respBody = s;
        return respHeader + "\r\n" + respBody + "\r\n";
    }

    public int getParts(char []dataArr, int i, String[] subArray){
        int j=0;
        while(i< dataArr.length && j < subArray.length){
            if(dataArr[i] == '$'){
                //bulk String
                //$<length>\r\n<data>\r\n
                i++;
                String partLength = "";
                while(i < dataArr.length && Character.isDigit(dataArr[i])){
                    partLength += dataArr[i];
                    i++;
                }
                i+=2;
                StringBuilder part = new StringBuilder();
                for(int k=0; k<Integer.parseInt(partLength);k++){
                    part.append(dataArr[i++]);
                }
                i+=2;
                subArray[j++]=part.toString();
            }
        }
        return i;
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
            i = lineEnd + 2 + parseLength(data, i + 1, lineEnd);
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
        int length = 0;
        for(int i = from; i < to; i++){
            length = length * 10 + (data[i] - '0');
        }
        return length;
    }

    public List<String[]> deseralize(byte[] command){
        return deseralize(command, 0, command.length);
    }

    /**
     * Decodes the RESP arrays held in data[from, to). Callers must pass whole frames:
     * trailing bytes that belong to the next array, or that have not arrived yet, are
     * the caller's to keep.
     */
    public List<String[]> deseralize(byte[] command, int from, int to){
        try{
            String data = new String(command, from, to - from, StandardCharsets.UTF_8);
            char[] dataArr = data.toCharArray();
            List<String[]> res = new ArrayList<>();

            int i=0;
            while(i < dataArr.length){

                char curr = dataArr[i];

                if(curr=='\u0000'){
                    break;
                }

                if(curr == '*'){
                    //array
                    String arrLen = "";
                    i++;
                    while(i < dataArr.length && Character.isDigit(dataArr[i])){
                        arrLen += dataArr[i++];
                    }
                    i+=2;
                    if(dataArr[i] == '*'){
                        // *2
                        // *3\r\n#3set\r\n#3key\r\n#5value
                        // *3\r\n#3set\r\n#3key\r\n#5value
                        for(int t=0;t<Integer.parseInt(arrLen);t++){
                            String nestedLen = "";
                            i++;
                            char c = dataArr[i];
                            while(i < dataArr.length && Character.isDigit(dataArr[i])){

                                nestedLen += dataArr[i++];
                            }
                            i+=2;
                            String[] subArray = new String[Integer.parseInt(nestedLen)];
                            i = getParts(dataArr, i, subArray);
                            res.add(subArray);
                        }
                    }else{
                        // *3\r\n#3set\r\n#3key\r\n#5value
                        String[] subArray = new String[Integer.parseInt(arrLen)];
                        i = getParts(dataArr, i, subArray);
                        res.add(subArray);
                    }
                }
            }
            return res;
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return new ArrayList<>();
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
            len = s.length();
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
