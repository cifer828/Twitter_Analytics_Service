package query3;

public class Content {
    private Long tid;
    // sender_uid and timestamp are used only for transforming the db schema
    private Long sender_uid;
    private String content;
    private Long timestamp;
    private Long favouriteCount;
    private Long retweetCount;
    private Long followerCount;

    Content(Long tid, Long sender_uid, Long timestamp, String content, Long favouriteCount, Long retweetCount, Long followerCount) {
        this.tid = tid;
        this.sender_uid = sender_uid;
        this.timestamp = timestamp;
        this.content = content;
        this.favouriteCount = favouriteCount;
        this.retweetCount = retweetCount;
        this.followerCount = followerCount;
    }

    public void setTid(Long tid) {
        this.tid = tid;
    }

    public Long getTid() {
        return tid;
    }

    public Long getSender_uid() {
        return sender_uid;
    }

    public void setSender_uid(Long sender_uid) {
        this.sender_uid = sender_uid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setFavouriteCount(Long favouriteCount) {
        this.favouriteCount = favouriteCount;
    }

    public Long getFavouriteCount() {
        return favouriteCount;
    }

    public void setRetweetCount(Long retweetCount) {
        this.retweetCount = retweetCount;
    }

    public Long getRetweetCount() {
        return retweetCount;
    }

    public void setFollowerCount(Long followerCount) {
        this.followerCount = followerCount;
    }

    public Long getFollowerCount() {
        return followerCount;
    }
}
