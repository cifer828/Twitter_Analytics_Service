/**
 * Class to manipulate json object.
 * All json reading operation should be included here
 */

package query1;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class JsonParser {
    JSONObject blockJson;

    /**
     * Constructor
     * @param blockJson input json of block chain
     */
    public JsonParser(JSONObject blockJson) { this.blockJson = blockJson; }

    /**
     *
     * @return the whole block json object
     */
    public JSONObject getBlockJson() { return blockJson; }

    /**
     *
     * @return a list of new transactions ("new_tx")
     */
    public JSONArray getNewTx() { return blockJson.getJSONArray("new_tx");}

    /**
     * @return a list of existing blocks ("chain")
     */
    public JSONArray getChain() { return blockJson.getJSONArray("chain"); }

    /**
     * @return get new_target from json
     */
    public String getNewTarget() { return blockJson.getString("new_target"); }

    /**
     * Get a block in chain using index
     *
     * @index position of block to be retrieved
     * @return block of current "chain" at certain index
     */
    public JSONObject getBlockAt(int index) {
        // get current blocks
        JSONArray blocks = blockJson.getJSONArray("chain");
        JSONObject lastBlock = (JSONObject) blocks.get(index);
        return lastBlock;
    }

    /**
     *
     * @return block size of current chain
     */
    public int blockSize() { return getChain().length(); }

    /**
     * Retrieve value of key in the json
     *
     * @param obj json object
     * @param key key to find
     * @return string value of key
     *         "" of key doesn't exist
     */
    public static String getOrEmpty(JSONObject obj, String key) {
        if (obj.has(key))
            return "" + obj.get(key);
        else
            return "";
    }


    /**
     * Decode request query to Json object
     * 1. decode message using base64
     * 1. unzip message into json
     *
     * @param encodedMsg request message encode with zlib and base64
     * @return Json object
     */
    public static JSONObject decode2Json(String encodedMsg){
        byte[] decoded = Base64.decodeBase64(encodedMsg);

        Inflater decompresser = new Inflater();
        decompresser.setInput(decoded);
        byte[] result = new byte[4096];
        int resultLength = 0;
        try {
            resultLength = decompresser.inflate(result);
            decompresser.end();
            String outputString = new String(result, 0, resultLength);
            return new JSONObject(outputString);

        } catch (DataFormatException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * Compress Json object into hex string
     *
     * @param json
     * @return
     */
    public static String compressJson(JSONObject json){
        String jsonStr = json.toString();
        byte[] output = new byte[1024];
        Deflater compresser = new Deflater();
        compresser.setInput(jsonStr.getBytes());
        compresser.finish();
        int compressedDataLength = compresser.deflate(output);
        compresser.end();
        byte[] b64data = Base64.encodeBase64URLSafe(Arrays.copyOfRange(output, 0, compressedDataLength));
        return new String(b64data);
    }

}
