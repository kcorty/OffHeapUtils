package org.example;

public class CharSequenceUtils {

    public static boolean equals(final CharSequence a, final CharSequence b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        final int length = a.length();
        if (length != b.length()) {
            return false;
        }

        for (int i = length - 1; i >= 0; i--) {
            if (a.charAt(i) != b.charAt(i)) {
                return false;
            }
        }

        return true;
    }
}
