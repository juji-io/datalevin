package datalevin.sm;

public interface StringDistance {

    /**
     * Calculates the distance between {@code string1} and {@code string2}, early stopping at {@code maxDistance}.
     * @param string1 first string
     * @param string2 second string
     * @param maxDistance distance at which the algorithm will stop early
     * @return distance between {@code string1} and {@code string2}, early stopping at {@code maxDistance}, or {@code -1} if {@code maxDistance} was reached
     */
    int distanceWithEarlyStop(String string1, String string2, int maxDistance);

    /**
     * @see StringDistance#distanceWithEarlyStop(String, String, int)
     * @param string1 first string
     * @param string2 second string
     * @return distance between {@code string1} and {@code string2}
     */
    default int distance(String string1, String string2){
        return distanceWithEarlyStop(string1, string2, Math.max(string1.length(), string2.length()));
    }
}
