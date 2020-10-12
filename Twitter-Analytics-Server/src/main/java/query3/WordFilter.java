package query3;

import java.util.HashSet;

public class WordFilter {

    public static String cleanContent(String content, HashSet<String> bannedWords) {

        // need to scan the
        char[] contentChar = content.toCharArray();
        int index = 0;
        StringBuilder str = new StringBuilder();
        while (index < contentChar.length) {

            // if the character is not an alphabet nor a number, then it is a split of word
            if (!Character.isAlphabetic(contentChar[index]) && !Character.isDigit(contentChar[index])) {
                // need to decide if the word needs to be censored
                if (bannedWords.contains(str.toString().toLowerCase())) {
                    // replace the word with censored ***
                    int start = index - str.length();
                    int len = str.length() - 2;
                    for (int i = 1; i <= len; i++) {
                        contentChar[start + i] = '*';
                    }

                }
                str = new StringBuilder();
            } else {
                str.append(contentChar[index]);
            }


            index++;
        }


        return new String(contentChar);
    }

}
