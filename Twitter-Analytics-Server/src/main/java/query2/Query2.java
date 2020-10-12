package query2;


import java.sql.*;
import java.util.*;

import static webApplication.WebController.TEAM_INFO;


/******************************************************************************
 *
 * @author Qiuchen Z.
 * @date 3/22/20
 ******************************************************************************/
public class Query2 {
    // request parameters
    private long uid;
    private String type;
    private String phrase;
    private String hashTag;
    // two types
    private final static String REPLY = "reply";
    private final static String RETWEET = "retweet";
    private final static String BOTH = "both";

    /**
     * Constructor
     *
     * @param uid     user_id
     * @param type    contact tweet type
     * @param phrase  percent-encoded
     * @param hashTag a hashTag, need to be converted to lower case
     */
    public Query2(long uid, String type, String phrase, String hashTag) {
        this.uid = uid;
        this.type = type;
        this.phrase = phrase;
        this.hashTag = hashTag.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Response to the client.
     * Including each valid contact user's latest screen name, description and contact tweet.
     * Ordered by their score and uid
     *
     * @return response string
     */
    public String response(){
        Map<Long, ContactInfo> contactMap = new HashMap<>();
        // get contact user info from database
        retrieveData(type, contactMap);
        List<ContactInfo> infoList = new ArrayList<>();
        // compute the score for each contact user
        for (ContactInfo cio: contactMap.values()) {
            cio.computeScore(hashTag, phrase);
            infoList.add(cio);
        }
        // sort the result and build response string
        Collections.sort(infoList);
        StringJoiner ret = new StringJoiner("\n");
        ret.add(TEAM_INFO);
        for (ContactInfo cio: infoList) {
            ret.add(cio.responseString());
        }
        return ret.toString();
    }

    /**
     * Retrieve data from database and store it to contact uid class
     *
     * @param type reply, retweet or both
     * @param contactMap <contact_uid, contact info> map to store all contact user information,
     */
    private void retrieveData(String type, Map<Long, ContactInfo> contactMap) {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        Connection conn = null;
        // use different prepared statement for different request type
        try {
            conn = JdbcUtils_DBCP.getConnection();
            if (type.equals(REPLY)) {
                stmt = JdbcUtils_DBCP.singleStatement(conn);
                stmt.setLong(1, uid);
                stmt.setInt(2, 0);
            }
            else if (type.equals(RETWEET)) {
                stmt = JdbcUtils_DBCP.singleStatement(conn);
                stmt.setLong(1, uid);
                stmt.setInt(2, 1);
            } else {
                stmt = JdbcUtils_DBCP.bothPreStatement(conn);
                stmt.setLong(1, uid);
            }
            rs = stmt.executeQuery();

            // get data from db
            while (rs.next()) {
                long contactUid = rs.getLong("contact_uid");
                String contactContent = rs.getString("content");
                String contactHashTags = rs.getString("hashTags");
                int interaction = rs.getInt("interaction");
                int hashTagCount = rs.getInt("hashtag_cnt");
                String screenName = rs.getString("screen_name");
                String description = rs.getString("description");
                String latestTweet = rs.getString("latest_tweet");
                ContactInfo cio = contactMap.get(contactUid);
                // update the contact info if that contact user already exists
                if (cio != null){
                    cio.update(contactHashTags, contactContent);
                } else {
                    // otherwise create new contact user with his/her info
                    contactMap.put(contactUid, new ContactInfo(contactUid, contactContent, contactHashTags, interaction, hashTagCount, screenName, description, latestTweet));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // !!!remember to release the connection and statement
            // otherwise it will exceed max connections
            JdbcUtils_DBCP.release(conn, stmt, rs);
        }
    }


    /**
     * Main test driver
     *
     * @param args not userd
     */
    public static void main(String[] args) {
        Query2 q2 = new Query2(2168934495L, RETWEET, "Make", "truth");
//        Query2 q2 = new Query2(224620498, BOTH, "pensions", "pensions");
//        Query2 q2 = new Query2(2246204187L, RETWEET, "مجرد", "مهرجان_الرياض_للتسوق5");
//        Query2 q2 = new Query2(2246206871L, "retweet", "fıtratında", "günkömürkarası");
        System.out.println(q2.response());
    }

}

