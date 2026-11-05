package com.tripbus.realtime.ws;

import java.util.regex.Pattern;

/** {@code seq} de un evento: el id de su entrada en el stream de Redis, {@code "<ms>-<n>"}. */
public final class Seq {

    private static final Pattern FORMAT = Pattern.compile("\\d{1,19}-\\d{1,19}");

    private Seq() {
    }

    /** true si tiene la forma {@code <ms>-<n>} con números que caben en un long. */
    public static boolean isValid(String seq) {
        if (seq == null || !FORMAT.matcher(seq).matches()) {
            return false;
        }
        try {
            parse(seq);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Compara numéricamente (primero los milisegundos, luego n), no como texto. Ambos deben ser válidos. */
    public static int compare(String a, String b) {
        long[] x = parse(a);
        long[] y = parse(b);
        int byMillis = Long.compare(x[0], y[0]);
        return byMillis != 0 ? byMillis : Long.compare(x[1], y[1]);
    }

    /** Milisegundos (la parte antes del guion). */
    public static long millis(String seq) {
        return parse(seq)[0];
    }

    private static long[] parse(String seq) {
        int dash = seq.indexOf('-');
        return new long[]{Long.parseLong(seq.substring(0, dash)), Long.parseLong(seq.substring(dash + 1))};
    }
}
