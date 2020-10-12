package query3;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class CalcScore {

    static final String STOPWORDS = "stopwords.txt";
    static final String BANNEDWORDS = "decrypted_list.txt";
    static final String URLPATTERN = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    // TODO: Update the word pattern
    static final String SPLITPATTERN = "[^a-zA-Z0-9'-]+";
    static final String WORDPATTERN = ".*[a-zA-Z]+.*";

    static HashSet<String> stopWords = loadWords(STOPWORDS);
    static HashSet<String> bannedWords = loadWords(BANNEDWORDS);
    HashSet<String> topKWordsSet = new HashSet<>();

    HashMap<Long, Long> impactMap;

//    HashMap<Long, ContentExtended> rawInput;
    HashMap<Long, Content> rawInput;


    int numOfTopicWords;
    int maxNumOfTweets;

    public CalcScore(HashMap<Long, Content> rawInput, int numOfTopicWords, int maxNumOfTweets) {
        this.rawInput = rawInput;
        this.impactMap = getImpactScore();
        this.numOfTopicWords = numOfTopicWords;
        this.maxNumOfTweets = maxNumOfTweets;
    }


    /**
     * The function that is used to get the top words.
     */
    /**
     * The function that is used to get the top words.
     */
    public String getTopWords() {
        // ============================ SLOW PART ===============================//
        long start = System.currentTimeMillis();
//        HashMap<String, Double> topWords = getTopicWordsNew();
        HashMap<String, Double> topWords = getTopicWords();
        long end = System.currentTimeMillis();
//        System.out.println("Time to get the topic words: " + (end - start) + " milliseconds");
        // ============================ SLOW PART ===============================//


        StringBuilder sb = new StringBuilder();
        // n1 topic words sorted by their topic scores in descending order,
        // breaking ties in ascending lexicographical order.
        // Note: please sort topic words first, and then do the rounding in the response.
        PriorityQueue<String> heap = new PriorityQueue<>((n1, n2) -> {
            if (!topWords.get(n1).equals(topWords.get(n2))) {
                return topWords.get(n1).compareTo(topWords.get(n2));
            } else {
                return n2.compareTo(n1);
            }
        });

        for (String word : topWords.keySet()) {
            // filter out the stop words
            if (!stopWords.contains(word)) {
                heap.add(word);
            }
            if (heap.size() > numOfTopicWords) {
                heap.poll();
            }
        }
        LinkedList<String> topKWords = new LinkedList<>();
        while (!heap.isEmpty()) {
            String word = heap.poll();
            topKWords.addFirst(word);
            topKWordsSet.add(word);

        }
        for (String word : topKWords) {
            sb.append(word).append(":").append(String.format("%.2f", topWords.get(word))).append("\t");
        }
        sb.deleteCharAt(sb.length() - 1);
        return WordFilter.cleanContent(sb.toString(), bannedWords);

    }

    /**
     * The function that is used to get the top tweets.
     */
    public String getTopTweets() {
        StringJoiner sj = new StringJoiner("\n");

        // A list of tweet text,
        // each of which contain ANY of these n1 topic words,
        // in descending order by their impact scores,
        // breaking ties by tweet id in descending order.
        PriorityQueue<Long> heap = new PriorityQueue<>((n1, n2) -> {
            if (impactMap.get(n1).compareTo(impactMap.get(n2)) != 0) {
                return impactMap.get(n1).compareTo(impactMap.get(n2));
            } else {
                return n1.compareTo(n2);
            }
        });

        for (Long tid : impactMap.keySet()) {
            String tweetContent = rawInput.get(tid).getContent();
            if (validTweet(tweetContent)) {
                heap.add(tid);
            }
            if (heap.size() > maxNumOfTweets) {
                heap.poll();
            }
        }
        LinkedList<Long> topK = new LinkedList<>();
        while (!heap.isEmpty()) {
            topK.addFirst(heap.poll());
        }
        for (Long tid : topK) {
            String filtered = WordFilter.cleanContent(rawInput.get(tid).getContent(), bannedWords);
            sj.add(impactMap.get(tid) + "\t" + tid + "\t" + filtered);
        }
        return sj.toString();
    }

    private boolean validTweet(String tweet) {
        String[] wordsInTweet = tweet.split(SPLITPATTERN);
        for (String word : wordsInTweet) {
            if (topKWordsSet.contains(word.toLowerCase())) return true;
        }
        return false;
    }


    public int getEwc(String content) {
        int result = 0;
        String[] words = content.split(SPLITPATTERN);
        for (String word : words) {
            if (isWord(word) && !stopWords.contains(word.toLowerCase())) result++;
        }
        return result;
    }

    /***
     *
     * Should only be used after splitting.
     */
    public boolean isWord(String text) {

        return text.matches(WORDPATTERN);
    }

    public HashMap<Long, Long> getImpactScore() {

        HashMap<Long, Long> tempMap = new HashMap<>();

        for (Long key : rawInput.keySet()) {
            Content content = rawInput.get(key);


            String tweet = content.getContent();
            String[] removeUrl = tweet.split(URLPATTERN);

            int ewc = 0;
            for (String str : removeUrl) {

                ewc += getEwc(str);

            }
            // count the score
            Long score = ewc * (content.getFavouriteCount() + content.getRetweetCount() + content.getFollowerCount());
            // put into the map
            tempMap.put(content.getTid(), score);

        }

        return tempMap;
    }

    /**
     * get Topic words and its score
     */
    public HashMap<String, Double> getTopicWords() {
        // HashMap that stores the tf map of each tweet
        HashMap<Long, HashMap<String, Double>> tfMap = new HashMap<>();
        // <Word, TweetCount> for all the tweets.
        HashMap<String, Integer> idfMap = new HashMap<>();

        int numOfTweets = rawInput.keySet().size();


        // Iterate through each tweet, build the idf map and tf map
        for (Long key : rawInput.keySet()) {
            // a temp map to store each word and its word count
            HashMap<String, Integer> wordCountMap = new HashMap<>();
            // Store each word and its tf score
            HashMap<String, Double> tfTweetMap = new HashMap<>();

            int totalWords = 0;

            Content content = rawInput.get(key);
            String tweet = content.getContent();
            // Remove the URL
            String[] removeUrl = tweet.split(URLPATTERN);
            // Build a hashset to store the distinct words.
            HashSet<String> wordSet = new HashSet<>();

            for (String str : removeUrl) {
                String[] words = str.split(SPLITPATTERN);
                // Iterate through all the words.
                for (String word : words) {
                    word = word.toLowerCase();
                    if (isWord(word)) {
                        totalWords++;
                        // update the count map
                        wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
                        // Put all the words of a tweet into a set
                        wordSet.add(word);
                    }
                }
            }

            // Build the tf map
            for (String str : wordCountMap.keySet()) {
                double tfScore = (double) wordCountMap.get(str) / totalWords;
                tfTweetMap.put(str, tfScore);
            }

            // Put the tweet map into whole tf Map
            tfMap.put(key, tfTweetMap);


            // Build the idfMap, every word should only be count once for a tweet in idf.
            for (String word : wordSet) {
                idfMap.put(word, idfMap.getOrDefault(word, 0) + 1);
            }
        }
        // calculate the idf score for each string.
        HashMap<String, Double> idfScoreMap = new HashMap<>();
        for (String str : idfMap.keySet()) {
            double numOfTweetsDouble = (double) numOfTweets;
            double idfCountDouble = (double) idfMap.get(str);
            double idfScore = Math.log(numOfTweetsDouble / idfCountDouble);
            idfScoreMap.put(str, idfScore);
        }

        // Build the ef-idfMap
        HashMap<String, Double> res = new HashMap<>();
        for (String word : idfMap.keySet()) {

            double idfScore = idfScoreMap.get(word);

            double topicScore = 0.0;
            for (Long tid : tfMap.keySet()) {
                HashMap<String, Double> tempTfMap = tfMap.get(tid);

                // tf_idf = tf * idf
                double x = tempTfMap.getOrDefault(word, 0.0) * idfScore;

                long y = impactMap.get(tid);

                topicScore += (double) x * Math.log((double) y + 1.0);

            }
            res.put(word, topicScore);
        }


        return res;
    }


    public static HashSet<String> loadWords(String file) {
        HashSet<String> set = new HashSet<>();
        Scanner sc;
        try {
            sc = new Scanner(new File(file));
            while (sc.hasNextLine()) {
                set.add(sc.nextLine().toLowerCase());
            }
            sc.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return set;

    }

}
