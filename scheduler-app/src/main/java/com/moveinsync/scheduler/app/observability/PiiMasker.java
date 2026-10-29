package com.moveinsync.scheduler.app.observability;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks PII (emails, latitude/longitude, phone numbers) in free text with fixed tokens.
 *
 * <p>Order: email, then geo, then phone. Every pattern is anchored by lookarounds so it never
 * matches inside a longer token (UUIDs, timestamps, fencing tokens, IPs, epoch millis), and uses
 * only bounded quantifiers without nested unbounded repetition, so it cannot backtrack
 * catastrophically.
 */
public final class PiiMasker {

    public static final String EMAIL_TOKEN = "[email]";
    public static final String GEO_TOKEN = "[geo]";
    public static final String PHONE_TOKEN = "[phone]";

    static final Pattern EMAIL = Pattern.compile(
            "(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]{1,63}(?:\\.[A-Za-z0-9-]{1,63}){0,8}"
                    + "\\.[A-Za-z]{2,24}(?![A-Za-z0-9-])");

    /** Decimal coordinate pair, at least 4 decimals on each side. */
    static final Pattern GEO_PAIR = Pattern.compile(
            "(?<![\\w.])-?\\d{1,3}\\.\\d{4,15}[ \\t]{0,3},[ \\t]{0,3}-?\\d{1,3}\\.\\d{4,15}(?!\\w|\\.\\d)");

    /** Keyed coordinate in JSON or key=value form; group 1 is the key part, group 2 the optional quote. */
    static final Pattern GEO_KEYED = Pattern.compile(
            "(?i)(\\b(?:latitude|longitude|lat|lng|lon|long)\\b[\"']?[ \\t]{0,3}[:=][ \\t]{0,3})"
                    + "([\"']?)-?\\d{1,3}(?:\\.\\d{1,15})?\\2(?!\\w|\\.\\d)");

    /** International number with leading '+': country code, optional (area), 2-4 digit groups. */
    static final Pattern PHONE_INTL = Pattern.compile(
            "(?<![\\w+])\\+\\d{1,3}(?:[ .-]?\\(\\d{1,4}\\))?(?:[ .-]?\\d{2,5}){2,4}(?!\\w|[.-]\\d)");

    /** Indian 10-digit mobile starting 6-9, optionally split 5+5. */
    static final Pattern PHONE_IN_MOBILE = Pattern.compile(
            "(?<![\\w.+-])[6-9]\\d{4}[ -]?\\d{5}(?!\\w|[.-]\\d)");

    /** US style: (415) 555-0100, 415-555-0100, 415.555.0100. */
    static final Pattern PHONE_US = Pattern.compile(
            "(?<![\\w.+-])(?:\\(\\d{3}\\) ?|\\d{3}[-.])\\d{3}[-.]\\d{4}(?!\\w|[.-]\\d)");

    /** Landline with trunk 0: 080-12345678, 022 23456789. */
    static final Pattern PHONE_LANDLINE = Pattern.compile(
            "(?<![\\w.+-])0\\d{2,4}[ -]\\d{6,8}(?!\\w|[.-]\\d)");

    private PiiMasker() {
    }

    /**
     * Returns {@code s} with PII replaced by tokens. Null-safe; returns the same instance when
     * nothing matches (fast path when the input has no '@' and no digit).
     */
    public static String mask(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        boolean hasAt = s.indexOf('@') >= 0;
        boolean hasDigit = containsDigit(s);
        if (!hasAt && !hasDigit) {
            return s;
        }
        String out = s;
        if (hasAt) {
            out = replace(EMAIL, out, EMAIL_TOKEN);
        }
        if (hasDigit) {
            out = replace(GEO_PAIR, out, GEO_TOKEN);
            out = replaceKeyedGeo(out);
            out = replaceIntlPhone(out);
            out = replace(PHONE_IN_MOBILE, out, PHONE_TOKEN);
            out = replace(PHONE_US, out, PHONE_TOKEN);
            out = replace(PHONE_LANDLINE, out, PHONE_TOKEN);
        }
        return out.equals(s) ? s : out;
    }

    private static String replace(Pattern pattern, String input, String token) {
        Matcher m = pattern.matcher(input);
        return m.find() ? m.replaceAll(Matcher.quoteReplacement(token)) : input;
    }

    private static String replaceKeyedGeo(String input) {
        Matcher m = GEO_KEYED.matcher(input);
        if (!m.find()) {
            return input;
        }
        return m.replaceAll(r -> Matcher.quoteReplacement(r.group(1) + r.group(2) + GEO_TOKEN + r.group(2)));
    }

    /** International matches must carry 8-15 digits (E.164 range) to be masked. */
    private static String replaceIntlPhone(String input) {
        Matcher m = PHONE_INTL.matcher(input);
        if (!m.find()) {
            return input;
        }
        return m.replaceAll(r -> {
            int digits = countDigits(r.group());
            return Matcher.quoteReplacement(digits >= 8 && digits <= 15 ? PHONE_TOKEN : r.group());
        });
    }

    private static boolean containsDigit(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                return true;
            }
        }
        return false;
    }

    private static int countDigits(String s) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                n++;
            }
        }
        return n;
    }
}
