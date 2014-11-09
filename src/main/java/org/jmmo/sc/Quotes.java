package org.jmmo.sc;

public class Quotes {
    private Quotes() {}

    public static final char QUOTE = '\"';

    public static String addQuotes(String string) {
        if (!string.isEmpty()) {
            if (string.charAt(0) == QUOTE) {
                if (string.length() > 1 && string.charAt(string.length() - 1) == QUOTE) {
                    return string;
                }
                else {
                    return string + QUOTE;
                }
            }

            if (string.charAt(string.length() - 1) == QUOTE) {
                return QUOTE + string;
            }
        }

        return QUOTE + string + QUOTE;
    }

    public static String removeQuotes(String string) {
        if (string.isEmpty()) {
            return string;
        }

        if (string.charAt(0) == QUOTE || string.charAt(string.length() - 1) == QUOTE) {
            if (string.length() >= 2 && string.charAt(0) == QUOTE && string.charAt(string.length() - 1) == QUOTE) {
                return string.substring(1, string.length() - 1);
            }
            else {
                throw new IllegalArgumentException("Single quote is found in " + string);
            }
        }

        return string;
    }
}
