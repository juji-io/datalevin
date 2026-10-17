package datalevin.search;

import datalevin.utl.PriorityQueue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * An in-memory UTF-8 radix trie shared by prefix and fuzzy fulltext queries.
 * Terminal nodes contain term IDs; LMDB remains authoritative for postings.
 * Immutable paths let readers traverse while indexing inserts new terms.
 */
public final class TermDictionary {
    private static final Node[] NO_CHILDREN = new Node[0];
    private volatile Node root = new Node(new byte[0], -1, NO_CHILDREN);

    private static final class Node {
        final byte[] label;
        final int termId;
        final Node[] children;

        Node(byte[] label, int termId, Node[] children) {
            this.label = label;
            this.termId = termId;
            this.children = children;
        }
    }

    /** One matching indexed term, with its actual edit distance. */
    public static final class Match {
        public final int termId;
        public final int distance;
        private final long ordinal;

        Match(int termId, int distance, long ordinal) {
            this.termId = termId;
            this.distance = distance;
            this.ordinal = ordinal;
        }
    }

    /** Thrown instead of returning incomplete results when work is exhausted. */
    public static final class WorkLimitException extends RuntimeException {
        public WorkLimitException(long limit) {
            super("Fulltext term expansion exceeded max-term-visits: " + limit);
        }
    }

    /** Thrown instead of silently truncating a prefix's live alternatives. */
    public static final class ExpansionLimitException extends RuntimeException {
        public ExpansionLimitException(int limit) {
            super("Fulltext prefix expansion exceeded max-prefix-expansions: " + limit);
        }
    }

    /** Add a term at the same visibility boundary as other search index state. */
    public synchronized void put(String term, int termId) {
        if (termId < 0) throw new IllegalArgumentException("negative term ID");
        byte[] key = term.getBytes(StandardCharsets.UTF_8);
        root = insert(root, key, 0, termId);
    }

    /** Clear alongside the engine's other in-memory indexes. */
    public synchronized void clear() {
        root = new Node(new byte[0], -1, NO_CHILDREN);
    }

    private static Node insert(Node node, byte[] key, int offset, int termId) {
        int common = 0;
        while (common < node.label.length && offset + common < key.length
                && node.label[common] == key[offset + common]) common++;
        if (common < node.label.length) {
            Node old = new Node(Arrays.copyOfRange(node.label, common, node.label.length),
                                node.termId, node.children);
            byte[] prefix = Arrays.copyOf(node.label, common);
            if (offset + common == key.length) {
                return new Node(prefix, termId, new Node[] {old});
            }
            Node fresh = new Node(Arrays.copyOfRange(key, offset + common, key.length),
                                  termId, NO_CHILDREN);
            Node[] children = (old.label[0] & 255) < (fresh.label[0] & 255)
                ? new Node[] {old, fresh} : new Node[] {fresh, old};
            return new Node(prefix, -1, children);
        }
        offset += common;
        if (offset == key.length) {
            return node.termId == termId ? node : new Node(node.label, termId, node.children);
        }
        int index = childIndex(node.children, key[offset] & 255);
        if (index >= 0) {
            Node child = insert(node.children[index], key, offset, termId);
            if (child == node.children[index]) return node;
            Node[] children = node.children.clone();
            children[index] = child;
            return new Node(node.label, node.termId, children);
        }
        int at = -index - 1;
        Node[] children = new Node[node.children.length + 1];
        System.arraycopy(node.children, 0, children, 0, at);
        children[at] = new Node(Arrays.copyOfRange(key, offset, key.length), termId, NO_CHILDREN);
        System.arraycopy(node.children, at, children, at + 1, node.children.length - at);
        return new Node(node.label, node.termId, children);
    }

    private static int childIndex(Node[] children, int firstByte) {
        int lo = 0, hi = children.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int value = children[mid].label[0] & 255;
            if (value < firstByte) lo = mid + 1;
            else if (value > firstByte) hi = mid - 1;
            else return mid;
        }
        return -lo - 1;
    }

    /** A shared work budget for every expanded leaf of one search request. */
    public static final class Budget {
        final long limit;
        long visits;
        public Budget(long limit) {
            if (limit <= 0) throw new IllegalArgumentException("max-term-visits must be positive");
            this.limit = limit;
        }
        void visit() {
            if (++visits > limit) throw new WorkLimitException(limit);
        }
    }

    /** Enumerate all live completions in UTF-8 lexical order. */
    public List<Match> prefix(String prefix, long maxVisits, IntPredicate live) {
        return prefix(prefix, new Budget(maxVisits), live);
    }

    public List<Match> prefix(String prefix, Budget budget, IntPredicate live) {
        return prefix(prefix, Integer.MAX_VALUE, budget, live);
    }

    /** Enumerate at most maxExpansions live terms, throwing if another exists. */
    public List<Match> prefix(String prefix, int maxExpansions, Budget budget,
                              IntPredicate live) {
        if (maxExpansions <= 0) {
            throw new IllegalArgumentException("max-prefix-expansions must be positive");
        }
        byte[] key = prefix.getBytes(StandardCharsets.UTF_8);
        Node node = root;
        int offset = 0;
        while (true) {
            int common = 0;
            while (common < node.label.length && offset < key.length) {
                budget.visit();
                if (node.label[common++] != key[offset++]) return List.of();
            }
            if (offset == key.length) break;
            int index = childIndex(node.children, key[offset] & 255);
            if (index < 0) return List.of();
            node = node.children[index];
        }
        List<Match> result = new ArrayList<>();
        completions(node, maxExpansions, budget, live, result);
        return result;
    }

    private static void completions(Node node, int maxExpansions, Budget budget, IntPredicate live,
                                    List<Match> result) {
        budget.visit();
        if (node.termId >= 0 && live.test(node.termId)) {
            if (result.size() == maxExpansions) throw new ExpansionLimitException(maxExpansions);
            result.add(new Match(node.termId, 0, result.size()));
        }
        for (Node child : node.children) completions(child, maxExpansions, budget, live, result);
    }

    private static final Comparator<Match> BEST_FIRST =
        Comparator.comparingInt((Match m) -> m.distance).thenComparingLong(m -> m.ordinal);

    private static final class Matches extends PriorityQueue<Match> {
        long ordinal;
        Matches(int limit) { super(limit); }
        @Override protected boolean lessThan(Match a, Match b) {
            return BEST_FIRST.compare(a, b) > 0;
        }
    }

    /**
     * Intersect the trie with Lucene-derived Levenshtein DFAs. UTF-8 bytes are
     * composed into code-point transitions, including when an edge splits a
     * multi-byte character. Results prefer smaller distances, then lexical order.
     */
    public List<Match> fuzzy(String term, int edits, boolean transpositions,
                             int maxExpansions, long maxVisits, IntPredicate live) {
        return fuzzy(term, edits, transpositions, maxExpansions, new Budget(maxVisits), live);
    }

    public List<Match> fuzzy(String term, int edits, boolean transpositions,
                             int maxExpansions, Budget budget, IntPredicate live) {
        if (edits < 0 || edits > 2) throw new IllegalArgumentException("max-edits must be 0..2");
        if (maxExpansions <= 0) throw new IllegalArgumentException("max-expansions must be positive");
        LevenshteinAutomata[] automata = new LevenshteinAutomata[edits + 1];
        for (int i = 0; i <= edits; i++) automata[i] = new LevenshteinAutomata(term, i, transpositions);
        Matches matches = new Matches(maxExpansions);
        intersect(root, automata, 0, 0, 0, 0, 0, budget, live, matches);
        List<Match> result = new ArrayList<>(matches.size());
        while (matches.size() > 0) result.add(matches.pop());
        result.sort(BEST_FIRST);
        return result;
    }

    private static void intersect(Node node, LevenshteinAutomata[] automata,
                                  int s0, int s1, int s2, int partial, int remaining,
                                  Budget budget, IntPredicate live, Matches matches) {
        int max = automata.length - 1;
        for (byte raw : node.label) {
            budget.visit();
            int b = raw & 255;
            if (remaining == 0) {
                if (b < 128) partial = b;
                else if (b < 224) { partial = b & 31; remaining = 1; continue; }
                else if (b < 240) { partial = b & 15; remaining = 2; continue; }
                else { partial = b & 7; remaining = 3; continue; }
            } else {
                partial = (partial << 6) | (b & 63);
                if (--remaining != 0) continue;
            }
            s0 = automata[0].step(s0, partial);
            if (max >= 1) s1 = automata[1].step(s1, partial);
            if (max >= 2) s2 = automata[2].step(s2, partial);
            if ((max == 0 && s0 < 0) || (max == 1 && s1 < 0) || (max == 2 && s2 < 0)) return;
            // A full nearest-term queue permits tightening the active radius.
            if (matches.size() == matches.maxSize()) {
                int competitive = matches.top().distance;
                if ((competitive == 0 && s0 < 0) || (competitive == 1 && s1 < 0)) return;
            }
        }
        budget.visit();
        if (node.termId >= 0 && remaining == 0) {
            int distance = automata[0].isAccept(s0) ? 0
                : max >= 1 && automata[1].isAccept(s1) ? 1
                : max >= 2 && automata[2].isAccept(s2) ? 2 : -1;
            long ordinal = matches.ordinal++;
            if (distance >= 0 && live.test(node.termId)) {
                matches.insertWithOverflow(new Match(node.termId, distance, ordinal));
            }
        }
        for (Node child : node.children) {
            intersect(child, automata, s0, s1, s2, partial, remaining, budget, live, matches);
        }
    }
}
