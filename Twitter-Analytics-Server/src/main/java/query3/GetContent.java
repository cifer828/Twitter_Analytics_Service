package query3;


import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import query2.JdbcUtils_DBCP;
import webApplication.WebController;

public class GetContent {

    public static Set<String> bannedWords = new HashSet<>();
    private static String TABLE_NAME = "query3_all";

    public static HashMap<Long, Content> getContents(Long timeStart, Long timeEnd, Long uidStart, Long uidEnd) {
        HashMap<Long, Content> contents = new HashMap<>();
        String query = "select * from " +
                TABLE_NAME +
                " where timestamp >= ? " +
                " and timestamp <= ? " +
                " and sender_uid >= ? " +
                " and sender_uid <= ? ";
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = JdbcUtils_DBCP.getConnection();
            stmt = conn.prepareStatement(query);
            stmt.setLong(1, timeStart);
            stmt.setLong(2, timeEnd);
            stmt.setLong(3, uidStart);
            stmt.setLong(4, uidEnd);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Long tid = rs.getLong("tid");
                Long sender_uid = rs.getLong("sender_uid");
                Long timestamp = rs.getLong("timestamp");
                String content = rs.getString("content");
                Long favourite_count = rs.getLong("favorite_count");
                Long retweet_count = rs.getLong("retweet_count");
                Long follower_count = rs.getLong("followers_count");

                contents.put(tid, new Content(tid, sender_uid, timestamp, content, favourite_count, retweet_count, follower_count));

            }
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        } finally {
            JdbcUtils_DBCP.release(conn, stmt, rs);
        }
        return contents;
    }

    /**
     * This function will fetch all the banned words from the file
     * decrypted_list.txt
     * it should be called when the service started
     */
    public static void getBannedWords() {
        // read in the files
        try {
            BufferedReader in = new BufferedReader(new FileReader("decrypted_list.txt"));
            String word = "";
            while ((word = in.readLine()) != null) {
                bannedWords.add(word);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found!" + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // this function is used to generate the transformed ROT13 files
    // do not need to be used in future scenario
    public void generateOriginalList(String fileName) {
        // read the file in
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            BufferedWriter writer = new BufferedWriter(new FileWriter("decrypted_list.txt", true));
            String encryptedWord = "";
            while ((encryptedWord = in.readLine()) != null) {
                String newWord = transform(encryptedWord);
                writer.write(newWord + "\n");
            }
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String transform(String origin) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < origin.length(); i++) {
            if (Character.isAlphabetic(origin.charAt(i))) {
                char ori = origin.charAt(i);
                char trans = (char) (ori + 13);
                if (trans > 122) {
                    int diff = trans - 122 - 1;
                    trans = (char) ('a' + diff);
                }
                result.append(trans);
            } else {
                result.append(origin.charAt(i));
            }
        }


        return result.toString();
    }

    public static void main(String[] args) {
        StringBuilder result = new StringBuilder();
        result.append(WebController.TEAM_INFO).append("\n");

        HashMap<Long, Content> rawInput = GetContent.getContents(1482940189L, 1483038959L, 3563725917L, 3563730530L);
        if (rawInput.size() == 0) {
            System.out.println(result.deleteCharAt(result.length() - 1).toString());
        }
        else {
            CalcScore calcScore = new CalcScore(rawInput, 10, 49);
            result.append(calcScore.getTopWords()).append("\n");
            result.append(calcScore.getTopTweets());
            System.out.println(result.toString());
        }

    }

}
