package query2;

import org.apache.commons.lang.StringUtils;

import java.util.Locale;

/******************************************************************************
 * Wrapper object of information of each contact user
 *
 * @author Qiuchen Z.
 * @date 4/23/20
 ******************************************************************************/
public class ContactInfo implements Comparable<ContactInfo>{
    long contactUid;
    String contactContent;
    String contactHashTags;
    int interaction;
    int hashTagCount;
    String screenName;
    String description;
    String latestTweet;
    double score;

    /**
     * Constructor
     *
     * @param contactUid contact user's uid
     * @param contactContent all the contact tweet combined in one string
     * @param contactHashTags all the hashTags in contact split by ,
     * @param interaction interaction count between request uid and contact uid
     * @param hashTagCount hashTag count between request uid and contact uid
     * @param screenName latest screen name of the contact user
     * @param description latest description of the contact user
     * @param latestTweet latest contact tweet of the contact user
     */
    ContactInfo(long contactUid, String contactContent, String contactHashTags,
            int interaction, int hashTagCount, String screenName, String description, String latestTweet){
        this.contactUid = contactUid;
        this.contactContent = contactContent;
        this.contactHashTags = contactHashTags.toLowerCase(Locale.ENGLISH);
        this.interaction = interaction;
        this.hashTagCount = hashTagCount;
        this.screenName = screenName == null ? "" : screenName;
        this.description = description == null ? "" : description;
        this.latestTweet = latestTweet == null ? "" : latestTweet;
    }

    /**
     * Add contact tweet content and hashTags to current info
     *
     * @param newHashTags hashTags to add
     * @param newContent tweet content to add
     */
    public void update(String newHashTags, String newContent){
        this.contactHashTags += newHashTags;
        this.contactContent += newContent;
    }

    /**
     * Compute the three scores
     *
     * @param targetHashTag request hashTag for keyword score
     * @param targetPhrase request phrase for keyword score
     */
    public void computeScore(String targetHashTag, String targetPhrase){
        int phraseMatch = StringUtils.countMatches(contactContent, targetPhrase);
        int hashTagMatch = StringUtils.countMatches(contactHashTags, targetHashTag);
        double keywordScore =  1.0 + Math.log(phraseMatch + hashTagMatch + 1);
        double hashTagScore = hashTagCount > 10 ? 1 + Math.log(1 + hashTagCount - 10) : 1.0;
        double interactionScore = 1 + Math.log(1 + interaction);
        this.score = keywordScore * hashTagScore * interactionScore;
    }

    /**
     * Generate the response string
     *
     * @return string to reply to client in terms of this contact user
     */
    public String responseString(){
        return contactUid + "\t" + screenName + "\t" + description + "\t" + latestTweet;
    }

    /**
     * sort by 1.score desc 2.uid desc
     *
     * @param cio another contact user's info
     * @return 1 if this contact user rank first
     *         -1 if another contact user rank first
     *         0 if ties
     */
    @Override
    public int compareTo(ContactInfo cio) {
        if (this.score == cio.score) {
            return contactUid > cio.contactUid ? -1 : 1;
        }
        else {
            return score > cio.score ? -1 : 1;
        }
    }
}
