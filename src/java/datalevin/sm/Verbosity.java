package datalevin.sm;

public enum Verbosity {
    /**
     * Top suggestion with the highest term frequency of the suggestions of smallest edit distance found
     */
    TOP,
    /**
     * All suggestions of smallest edit distance found
     */
    CLOSEST,
    /**
     * All suggestions within {@link SymSpell#getMaxDictionaryEditDistance}
     */
    ALL
}
