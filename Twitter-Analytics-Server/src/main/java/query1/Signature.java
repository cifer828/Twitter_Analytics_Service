/**
 * Implement methods for sign and verify signature
 *
 */
package query1;

import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Signature {
    public static BigInteger n = new BigInteger("1561906343821");
    public static BigInteger d = new BigInteger("343710770439");
    public static BigInteger e = new BigInteger("1097844002039");

    /**
     * Generate sha-256
     *
     * @param rawMsg string to hash
     * @return sha-256 hash
     */
    public static String sha256(String rawMsg){
        try {
            MessageDigest md = MessageDigest.getInstance("sha-256");
            md.update(rawMsg.getBytes());
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toLowerCase();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Overload: Check rsa validation using a tx json
     *
     * @param tx transaction to be verified
     * @return true if tx sig can be verified or tx has not sig
     *         false if tx sig cannot be verified
     */
    public static boolean verifyRSA(JSONObject tx){
        String txHash = JsonParser.getOrEmpty(tx, "hash");
        if (txHash.equals(""))
            return true;
        String txSig = JsonParser.getOrEmpty(tx, "sig");
        String txSend = JsonParser.getOrEmpty(tx, "send");
        return verifyRSA(txSig, txSend, txHash);
    }

    /**
     * Overload: Check rsa validation using raw message
     *
     * @param encryptedHashStr signature to verify
     * @param senderId sender id which is also the public key
     * @param hashToVerify hash of the tx which should be the decrypted message of signature
     * @return true if sig can be verified
     *         false if not
     */
    public static boolean verifyRSA(String encryptedHashStr, String senderId, String hashToVerify){
        // Take the encrypted string and make it a big integer
        BigInteger e = new BigInteger(senderId);
        BigInteger encrypted = new BigInteger(encryptedHashStr);
        // Decrypt it
        BigInteger decryptedHash = encrypted.modPow(e, n);
        BigInteger original = new BigInteger(hashToVerify,16);
        // Return the comparision result.
        return decryptedHash.equals(original);
    }

    /**
     * Sign the transaction using its hash
     *
     * @param transactionHash transaction to be signaed
     * @return signature
     */
    public static long signRSA(String transactionHash){
        BigInteger transactionBigInt = new BigInteger(transactionHash,16);
        String signature = transactionBigInt.modPow(d, n).toString();
        return Long.parseLong(signature);
    }
}
