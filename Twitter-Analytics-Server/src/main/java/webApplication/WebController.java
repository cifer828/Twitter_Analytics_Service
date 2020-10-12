package webApplication;

import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import query1.BlockParser;
import query1.JsonParser;
import query2.Query2;
import query3.CalcScore;
import query3.Content;
import query3.GetContent;

import java.util.HashMap;

@RestController
public class WebController {
    public static final String TEAM_INFO = "Ying_Liu_Zhi_Zhu,351449476827";

    @RequestMapping("/q1")
    public String q1Response(@RequestParam("cc") String rquestMsg) {
        try {
            // IOC
            JSONObject blockJson = JsonParser.decode2Json(rquestMsg);
            JsonParser jsonParser = new JsonParser(blockJson);
            BlockParser blockParser = new BlockParser(jsonParser);
            String response;
            if (blockParser.validate()) {
                JSONObject responseJson = blockParser.responseJson();
                response = "Ying_Liu_Zhi_Zhu,351449476827\n" + JsonParser.compressJson(responseJson);
            } else {
                response = "Ying_Liu_Zhi_Zhu,351449476827\n" + "INVALID";
            }
            return response;
        } catch (Exception e) {
            return "Ying_Liu_Zhi_Zhu,351449476827\n" + "INVALID" + "\n" + e.getMessage();
        }
    }

    @RequestMapping("/q2")
    public String q2Response(@RequestParam("user_id") String userId,
                             @RequestParam("type") String type,
                             @RequestParam("phrase") String phrase,
                             @RequestParam("hashtag") String hashtag) {

        // MySQL
        try {
            Query2 re = new Query2(Long.parseLong(userId), type, phrase, hashtag);
            return re.response();
        } catch (Exception e) {
            return TEAM_INFO;
        }
    }


    @RequestMapping("/q3")
    public String q3Response(@RequestParam("uid_start") String uid_start,
                             @RequestParam("uid_end") String uid_end,
                             @RequestParam("time_start") String time_start,
                             @RequestParam("time_end") String time_end,
                             @RequestParam("n1") String topicWordLimit,
                             @RequestParam("n2") String tweetLimit) {
        try {
            StringBuilder result = new StringBuilder();
            result.append(TEAM_INFO).append("\n");

            HashMap<Long, Content> rawInput = GetContent.getContents(Long.parseLong(time_start),
                    Long.parseLong(time_end), Long.parseLong(uid_start), Long.parseLong(uid_end));
            if (rawInput.size() == 0) {
                return result.deleteCharAt(result.length() - 1).toString();
            }
            CalcScore calcScore = new CalcScore(rawInput, Integer.parseInt(topicWordLimit), Integer.parseInt(tweetLimit));

            result.append(calcScore.getTopWords()).append("\n");
            result.append(calcScore.getTopTweets());
            return result.toString();
        } catch (Exception e) {
            return TEAM_INFO;
        }
    }

    @RequestMapping("/")
    public String emptyResponse() {
        return TEAM_INFO;
    }
}
