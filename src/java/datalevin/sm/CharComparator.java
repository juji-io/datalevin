package datalevin.sm;

public class CharComparator {

    public boolean areEqual(char ch1, char ch2) {
        return ch1 == ch2;
    }

    public boolean areDistinct(char ch1, char ch2) {
        return !areEqual(ch1, ch2);
    }
}
