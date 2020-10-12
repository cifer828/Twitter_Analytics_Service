/**
 * Implement method to verify block,
 * compute pow and generate new block based new_tx
 */

package query1;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;


public class BlockParser {
    private JsonParser jsonParser;
    //  get account number (e in public key)
    private long account = Long.parseLong(Signature.e.toString());
    private Map<Long, Integer> balanceMap = new HashMap<>();
    private int currentReward = 500000000;
    // whether the next reward should be same as this.reward
    private boolean sameRewardNext = true;
    private long timeStamp = 0;

    /**
     * Constructor
     * @param jsonParser parser of json object
     */
    public BlockParser(JsonParser jsonParser) {
        this.jsonParser = jsonParser;
    }

    /**
     * Check hash correctness and verify signature
     * will traverse through all the blocks to verify each one of them
     *
     * checklist:
     * tx level:
     * 1. "hash" in tx = CCHash("timestamp|sender|recipient|amount|fee")
     * 2. "sig" in tx can be verified using "send", "hash" and n  (hard check)
     * 3. enough balance for each "send"er
     * 4. fee should not be negative
     * 5. amt should not be negative
     * 10. timestamp should monotonically increasing
     *
     * block level:
     * 6. reward amount follow 5, 5, 2.5, 2.5, 1.25, 1.25... pattern
     * 7. block hash = CCHash(SHA-256("block_id|previous_block_hash|tx1_hash|tx2_hash|tx3_hash...") + PoW)
     * 8. block hash < target
     * 9. id increment one by one
     *
     * @return true if valid
     *         false if not
     */
    public boolean validate() {
        return easyValidateChain() && middleValidateChain() && hardValidateChain();
    }

    /**
     * Easy check for old tx ("chain" in the json)
     * Check hash correctness and verify signature
     * will traverse through all the blocks to verify each one of them
     *
     * @return true if valid
     *         false if not
     */
    private boolean easyValidateChain() {
        int amt = 500000000;
        JSONArray chain = jsonParser.getChain();

        // easy check: 3, 4, 5, 10;
        for (int i = 0; i < chain.length(); i++) {
            JSONObject block = (JSONObject) chain.get(i);
            int blockID = block.getInt("id");

            // check rule 9: id increment
            if (blockID != i)
                return false;

            // get all tx hashes
            JSONArray txs = block.getJSONArray("all_tx");
            for (Object txObj: txs) {
                JSONObject tx = (JSONObject)txObj;
                // easy check tx validation
                // rule 3 balance +
                // rule 4 non-negative fee + rule 5 non-negative amt
                if (!easyCheckTx(tx))
                    return false;
                amt = tx.getInt("amt");
            }

            // rule 6 reward
            if (amt != getCurrentReward())
                return false;
        }

        return true;
    }


    /**
     * Check some easy rules that cost less time (no sig and hash check)
     *
     * @param tx transaction in json
     * @return true if valid
     *         false if not
     */
    private boolean easyCheckTx(JSONObject tx) {
        // check rule 5: non-negative integer fee
        if (tx.has("fee") && tx.getInt("fee") < 0) {
            return false;
        }
        //  check rule 6: non-negative amt
        if (tx.has("amt") && tx.getInt("amt") < 0)
            return false;
        // check rule 10: increasing timestamp
        long newTimeStamp = Long.parseLong(tx.getString("time"));
        if (newTimeStamp < timeStamp)
            return false;
        timeStamp = newTimeStamp;

        // check rule 4: balance
        if (!checkAndUpdateBalance(tx))
            return false;

        return true;
    }

    /**
     * Check sender's balance, then update sender & receiver balance
     *
     * @param tx transaction in json
     * @return true if valid
     *         false if not
     */
    private boolean checkAndUpdateBalance(JSONObject tx) {
        // get amount of this transaction
        int amt = tx.getInt("amt");
        // not a reward tx
        if (tx.has("send")) {
            Long send = tx.getLong("send");
            Integer sendBalance = balanceMap.get(send);
            // sender don't have enough money
            if (sendBalance == null || sendBalance < amt)
                return false;
            // deduct sender's money
            balanceMap.put(send, sendBalance - amt);
        }

        // add receiver's money
        Long recv = tx.getLong("recv");
        balanceMap.put(recv, balanceMap.getOrDefault(recv, 0) + amt);
        return true;
    }

    /**
     * Middle validation for old blocks (hash check for tx and block)
     *
     * @return true if valid
     *         false if not
     */
    private boolean middleValidateChain(){
        // the chain of blocks, first verify if the hash of the block is correct
        String preBlockHash = "";

        JSONArray chain = jsonParser.getChain();

        // easy check: rule 1
        for (int i = 0; i < chain.length(); i++) {
            JSONObject block = (JSONObject) chain.get(i);
            int blockID = block.getInt("id");
            String blockHash = JsonParser.getOrEmpty(block, "hash");
            String pow = JsonParser.getOrEmpty(block, "pow");

            // get prev hash of the first block
            if (blockID == 0) {
                preBlockHash = "00000000";
            }

            // build block info to hash
            StringJoiner blockInfo = new StringJoiner("|");
            blockInfo.add("" + blockID);
            blockInfo.add(preBlockHash);

            // get all tx hashes
            JSONArray txs = block.getJSONArray("all_tx");
            for (Object txObj: txs) {
                JSONObject tx = (JSONObject)txObj;
                // middle check tx validation
                if (!middleCheckTx(tx))
                    return false;
                blockInfo.add(JsonParser.getOrEmpty(tx, "hash"));
            }

            // rule 7 & 8 block hash
            String shaBlockInfo = Signature.sha256(blockInfo.toString());
            String digest2CCHash = shaBlockInfo + pow;
            String computeBlockHash = ccHash(digest2CCHash);
            if (!blockHash.equals(computeBlockHash) || blockHash.compareTo(block.getString("target")) > 0) {
                return false;
            }

            preBlockHash = blockHash;
        }

        return true;
    }

    /**
     * Middle check for each tx (hash check)
     *
     * @param tx transaction in json
     * @return true if valid
     *         false if not
     */
    private boolean middleCheckTx(JSONObject tx) {
        // check rule 1: tx hash
        String computeCCHash = ccHash(tx);
        String txCCHash = JsonParser.getOrEmpty(tx, "hash");
        return txCCHash.equals(computeCCHash);
    }

    /**
     * Hard check for current block chain (sig check)
     *
     * @return true if valid
     *         false if not
     */
    private boolean hardValidateChain() {
        JSONArray chain = jsonParser.getChain();

        for (int i = 0; i < chain.length(); i++) {
            JSONObject block = (JSONObject) chain.get(i);

            // get all tx hashes
            JSONArray txs = block.getJSONArray("all_tx");
            for (Object txObj: txs) {
                JSONObject tx = (JSONObject)txObj;
                // check rule 3: tx sig
                if (tx.has("sig") && !Signature.verifyRSA(tx)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     *
     * @return json object to response
     */
    public JSONObject responseJson(){
        // get current blocks
        JSONArray blocks = jsonParser.getChain();
        blocks.put(generateNewBlock());
        jsonParser.getBlockJson().remove("new_tx");
        jsonParser.getBlockJson().remove("new_target");
        return jsonParser.getBlockJson();
    }

    /**
     * Generate a new block to be added to the block chain
     * Updated new_tx and our reward tx in this new block
     *
     * @return a new block in chain
     */
    private JSONObject generateNewBlock() {
        // update the last transaction in the new block
        JSONArray newTx = jsonParser.getNewTx();

        // update information of the last tx in newTx
        for (Object jsonObj: newTx) {
            JSONObject oneNewTx = (JSONObject)jsonObj;
            updateNewTx(oneNewTx);
        }

        // add a new transaction indicates our mining in the new block
        JSONObject miningTx = new JSONObject();
        miningTx.put("amt", getCurrentReward());
        miningTx.put("recv", account);
        miningTx.put("time", newCoinTime());
        miningTx.put("hash", ccHash(miningTx));
        newTx.put(miningTx);

        // generate a new block
        JSONObject newBlock = new JSONObject();
        newBlock.put("all_tx", newTx);
        newBlock.put("id", newId());
        String target = jsonParser.getNewTarget();
        newBlock.put("target", target);

        // calculate sha256 part of new block's hash
        String lastBlockHash  = "00000000";
        if (jsonParser.blockSize() > 0) {
            JSONObject lastBlock = jsonParser.getBlockAt(jsonParser.blockSize() - 1);
            lastBlockHash = JsonParser.getOrEmpty(lastBlock, "hash");
        }
        String shaBlockHash = blockShaHash("" + newId(), lastBlockHash, newBlock);

        // compute pow
        String pow =computePow(shaBlockHash, target);
        newBlock.put("pow", pow);
        newBlock.put("hash", ccHash(shaBlockHash + pow));

        return newBlock;
    }

    /**
     * Update a tx in new_tx
     *
     * @param oneNewTx a tx to be updated
     */
    private void updateNewTx(JSONObject oneNewTx) {
        if (oneNewTx.has("sig"))
            return;
        // add account number
        oneNewTx.put("send", account);
        // add fee if no exists
        if (!oneNewTx.has("fee"))
            oneNewTx.put("fee", 0);
        // add hash to new block
        String txHash = ccHash(oneNewTx);
        oneNewTx.put("hash", txHash);
        oneNewTx.put("sig", Signature.signRSA(txHash));
    }


    /**
     * Get the id of new block
     *
     * @return new block id
     */
    private int newId(){
        // get last block
        JSONArray blocks = jsonParser.getChain();
        JSONObject lastBlock = (JSONObject) blocks.get(blocks.length() - 1);
        return lastBlock.getInt("id") + 1;
    }

    /**
     * Get current reward and set next reward
     *
     * @return reward of our mining
     */
    private int getCurrentReward() {
        int temp = currentReward;
        if (!sameRewardNext)
            currentReward /= 2;
        sameRewardNext = !sameRewardNext;
        return temp;
    }

    /**
     * Compute newCCCoin generation time
     * 10min after the last miner
     *
     * @return timestamp
     */
    private String newCoinTime() {
        // get last block
        JSONObject lastBlock = jsonParser.getBlockAt(jsonParser.blockSize() - 1);
        // get last transaction
        JSONArray transactions = lastBlock.getJSONArray("all_tx");
        JSONObject lastTx = (JSONObject) transactions.get(transactions.length() - 1);
        BigInteger lastTime = new BigInteger(lastTx.getString("time"));
        // add 10 minutes
        BigInteger newTime = lastTime.add(new BigInteger("600000000000"));
        return newTime.toString();
    }

    /**
     * Generate ccHash from block
     * CCHash("timestamp|sender|recipient|amount|fee")
     * CCHash is the first 8 char of sha256 hash.
     *
     * @return ccHash
     */
    private String ccHash(JSONObject tx){
        String hash = Signature.sha256(String.format("%s|%s|%s|%s|%s",
                JsonParser.getOrEmpty(tx, "time"),
                JsonParser.getOrEmpty(tx, "send"),
                JsonParser.getOrEmpty(tx, "recv"),
                JsonParser.getOrEmpty(tx, "amt"),
                JsonParser.getOrEmpty(tx, "fee")));
        return hash.substring(0, 8);
    }

    /**
     * Generate ccHash from string
     *
     * @param rawMsg message to hash
     * @return ccHash
     */
    private static String ccHash(String rawMsg) {
        String sha256Str = Signature.sha256(rawMsg);
        return sha256Str.substring(0, 8);
    }

    /**
     * A whole block hash is
     * CCHash(SHA-256("block_id|previous_block_hash|tx1_hash|tx2_hash|tx3_hash...") + PoW)
     * This method compute the SHA-256() part
     *
     * @param id block id
     * @param block block to compute hash
     * @return SHA-256() part of a block hash
     */
    private String blockShaHash(String id, String prevBlockHash, JSONObject block) {
        StringJoiner sj = new StringJoiner("|");
        sj.add(id);
        sj.add(prevBlockHash);
        for (Object obj: block.getJSONArray("all_tx")){
            sj.add(JsonParser.getOrEmpty((JSONObject)obj, "hash"));
        }
        return Signature.sha256(sj.toString());
    }

    /**
     * Compute the proof of work
     * CCHash(SHA-256("block_id|previous_block_hash|tx1_hash|tx2_hash|tx3_hash...") + PoW)
     *
     * @param ccHash first part of block hash
     * @param target target hash which the block hash should be lexicographically smaller than
     * @return block hash
     */
    private String computePow(String ccHash, String target){
        int pow = 0;
        String newHash = Signature.sha256(ccHash + pow);
        while (newHash.compareTo(target) > 0) {
            pow++;
            newHash = Signature.sha256(ccHash + pow);
        }
        return "" + pow;
    }

    /**
     * Write json result of request and response from test into file
     */
    public static void parseTest(){
        try {
            BufferedReader requestBr = new BufferedReader(new FileReader("testResult/request"));
            BufferedWriter requestBw = new BufferedWriter(new FileWriter("testResult/requestJson.json"));
            BufferedReader responseBr = new BufferedReader(new FileReader("testResult/response"));
            BufferedWriter responseBw = new BufferedWriter(new FileWriter("testResult/responseJson.json"));
            String line;
            StringBuilder request = new StringBuilder();
            while ((line = requestBr.readLine()) != null){
                request.append(line);
            }
            requestBw.write(JsonParser.decode2Json(request.toString()).toString());

            String response = "";
            while ((line = responseBr.readLine()) != null){
                response += line;
            }
            responseBw.write(JsonParser.decode2Json(response.toLowerCase()).toString());

            requestBr.close();
            requestBw.close();
            responseBr.close();
            responseBw.close();
        }
        catch (IOException e){

        }
    }

    public static void main(String[] args) {
        String requestMsg = "eJyFk9tum0AQht9lr7mYw85heZWqigBDbClxqxg1lSK_ewcCBddJuldoF3a--f7hLXXH5nRO9be31Dw9PYy_58eXvvuVai-SRd3Ji1WpeR5TLbCsKo2n5z7VCcVJCDJsK1Xp2FyOcZhbMnOFdP1epZ8_XmNrOj0dUg1_XwLrilu2OBibl8d-nG5N1-qG6HJ6TDUKcRCwBVepVkpGViRAp9gb-oCiDL4AI6lnL1nvgKc7TAiLaVS-9OfDvx0vfB1k46Y9zEjvNZUwxx1MbOU_ZhCA0OMGl4KbmaYVHfDgOzMEtLjBzQ2IDAeUvRv4WE60D0ScobCtbgqIFQgHBVc35kiyMGc3LYT3xIWyUg4g2dTcaF7z1VZx8H4GmimoCDmia5kieKdAKOY5JiSMrRiwIDgbSfF7BlSPtDFagq_jabXDFmHYxXPb9lyH5JN4FBhIVQ2Ut3haMAn1sosH13Rolw65N-w36TTzN-f-9WHbg2m45639NEfpQgaCRKsqFVcjN2bgxRQL69IEZ1K2fCdLWZkMgxf168BwaLN3hjtZH_zln9bJYjm7ksI0uNc_g4cQeA==";
        String negativeAmt = "eJx9kNtqwzAMQP9FzxlYkmVZ-ZUxipc4baDtRhPWQem_z-tM065jejK6WEfnBN0mjXton0-QttvV_Hl5HnL3AS060-i9c-TYGki7GVpxNRqYx12GFlAiCTnvloAGNmnalCJZfu2UDc4vDby_HUvKcymPPbTu2uXQ4iCaS2FOh3Wev3MZzs0d1DSuCxOJimB04pCbChopCCoio8cGhlywGCmy10r9JKWd9JE5uhgxKKqW3VPe9w9XV0SOQqGLP1B_bf3fjpkJ-1g-trjYSWkYei_djR0M1Q4udjhpKrvv7ITLzD4fV0uuh5q58WVKnjk4pWBXXVYuI2Ir6FUXIXFgX88gZgtq8usKdEQBWaRM89XXvYbKnHkIQ-JYKM9fnYectA==";
        String negativeFee = "eJyNlOtumzAUx9_Fn5l0rj4-eZVpqoCYJlLbTU20Tqry7jtQEnJTNosPyAb8-1_MZ-o37fYtrb5_pvbl5Wn_Z7p9r_3vtEJwKyIABOxNal_3aaUwjybtt681rRJqISUQWEZq0qbdbWKRvHa9safDjyb9-vkRU8KxvF2nFZyeAvQyqNVY2Lfvz3U_ztV0aC6gdtvntMqijuqSA62ZObO5qlNwOjZpqHVEZ0OkGZogXoGMN8xohcHBpFDsvatv6xvVM2KrFZF7mqAmEjIOFnb3fCIpOT5mKCjFZpKSqcDRPM5kekPB7FmzYkY8UVxqmiEqtz10tZsg7u34OCL3AqKEqKJLRDbUdug7O4sIyxzRsjWMWyP1_xERObHCKKfQyRgsLCaTqGNEZI6juAlaCsYo18wIaoAGmTDnkzmXomdCpjVlET8352LXryrofXOiIFaiI_GJKYVj7KQ9U0fn5sze0OKNKCOHg-ferO96o4DR9cg1Mz8oDQlDgM_MUY-slOkaOWs8A6Yqzo-9ifTWhuRLe-McMQSMkfKDc_Qtk4YtM0gECi7XBY5k4oqEiqE_LnDXQbYy2FlGJjiylNDxr4wIPYdU4yiFLxkNXvteer9XYF4sgEAP2y9CKtM7b_Xj6TQ3xjtNnKXGYVOJi0Jmc5f7q9EanOXoFRcklmsJcdwzFML47chjr3SthXsZEQ9_AfMGa0M=";
        String blockHashLargerThanTarget = "eJyVkNFqwzAMRf9Fz3mQbCWy8yujFMd2m0Cbjca0g5J_n2PSZqV7mZ7MvZY4nDv43g0jtB93cKfTPn2X5yX6K7SEVgwzokJtK3DnBG2N61SQhnOEFqg2qlbIuA1U0Lupz6WysfOiLcy7Cr4-bzlineshQIvPX0jWHGqJuUjucoxpyQLM1QvUNBwXJkZSpGvijPQn5yFmLM5IqqGVmpRSTJreoJnEahFjC9UUx_B2bmUUIfI-dIXq_34oR0aZhlmo2fxI533sbPfLz0MPbXq8i9IEetGDSGVpjLf9li4KS7Io280_y1h8pg==";
        String jumpId = "eJydktFOwzAMRf_Fz32wYzuO-ysITW2XsUkwEKsACe3fyUq7tgwJiUiVmqSpj8_NJ3T75nCE-u4TmsfHTf8xvL7m7g1qQrckghiQvYLmqYdacRwV9IenDDWQpqABBecBFeyb075sBs9tZ-xwvq_g5fm9LAmX7cMWarx-heRpp5bLRt-8PuT-8ls4Vyum0-EB6oQxqgsmlepKScaBPYoTVbDLBYpEo1-ZCZkJkW-QSSQVbkvxwnTKx-1N0yOhtCQ5MA5QA4mwlgqaYhSbUJK4I5Gix4kkcPCJww2F4w2FolEk8zJZUKyamjxZbKJ324HiHxm5J2Istj3xnFHqcix24iIjszGjRW29NGZhmRE2v4ZUImLRYAnV55QMS3hSHrXJTalqNlFzCFZm9gO6hOdu3xdM_kiJjcVZdis_q7pDpaC_-yF1j0GClIo6--k69Xbb2cIPpdEPz34atdbbtZ92OHPM75vFvR4XLsLuz19xWOJ8";
        String decreasingTime = "eJx9kNFqwzAMRf9Fz36QJduy8yujFMdxmkCbjSasg9J_nxvM0rAxPZl7LXx87pCGOE7QvN0hns_H5Ws9XnP6hEZjEG8MIiEHBfGyQGOxjoJlvGRoQFtPltDgNqBgiPNQSgq5TcIBHgcFH--3Ehku9dhBgz-3UAffW8mlWOL1lJdnVnbUDmoeT4WJDLFlssGzVRVUyGjHPhB7VtDngkWaHTus1CZodKL9C7SgIfR76DlP3a9vV0aTE7ZtLyvVX8_-q0cjaqOtExGkTU-O3klrulc91Y7e7NgUknjZ2UnrypRvxy2LUJOnr8PjGzIle5Y=";
        String floatFee = "eJyFk9tum0AUAP9ln1F1Lnsuy3u_oqoiwBBbStIqtppKkf-9xw4EsB2XJ8TC7uzM8p66bbN7SfWP99Q8PT0c_p5vX_vuT6q9SBZ1Jy9Wpeb5kGqB8arSYffcpzqhOAlBhvlKVdo2-20M5pbMXCEdf1bp96-3eHQa3W1SDZ8vgXXFLVsMHJrXx_5wmjUdqxXRfveYahTiIGALrlJNlIysSIBO8WzoA4oy-AiMpJ69ZL0CPs1hQlhMY-V9_7K53PHI10E2btrNGeljTSXMMQcTW_mPGQQg9JjBpeBspmlFB9z4wgwBjW5wdgMiwwZl6QZuy4ntAxFnKGyTmwJiBcJBwcmNOZKMzNlNC-E1caGslANIZjUrzVNfbRUH789AZwoqQo7oWk4JPigQinmOExLGJgwYEfwbG0lxlO92xYHqURxjW3A_UasdtgjDItF66-e1SL5IpMBAqmqgPCdqwST0yyIRToVoUYjcG_ZVoeb28Y11ChkIEk1uVFyN3JiBRzUsrCMxZ1K2fFVIWZkMAw71fiEc2uyd4VwI4--g2GhoRaxu_uqrQl8yZLGcXUnBPxkuUk-HnaTFoSuLPBcv3u0T-W_1wb4hLp4XfcLkGIgXgdCKtN0qEFh8dfwHL6A-pw==";

        JSONObject blockJson = JsonParser.decode2Json(requestMsg);
        String compress = JsonParser.compressJson(blockJson);

        JsonParser jsonParser = new JsonParser(blockJson);
        BlockParser blockParser = new BlockParser(jsonParser);
        if (blockParser.validate())
            System.out.println(blockParser.responseJson());


    }

}
