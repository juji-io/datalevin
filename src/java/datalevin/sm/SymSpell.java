/**
    MIT License

    Copyright (c) 2019 Wolf Garbe
    Copyright (c) 2019 Raul Garcia

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

/**
   Modified from JSymSpell https://github.com/rxp90/jsymspell
   which is a Java port of SymSpell https://github.com/wolfgarbe/SymSpell
*/

package datalevin.sm;

import datalevin.sm.SuggestItem;
import datalevin.sm.DamerauLevenshteinOSA;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;

import static datalevin.sm.Verbosity.*;

public class SymSpell {

    private static final long BIGRAM_COUNT_MIN = Long.MAX_VALUE;

    private final int maxDictionaryEditDistance;
    private final int prefixLength;

    /**
     * Map of Delete -> Collection of words that lead to that edited word
     */
    private final Map<String, Collection<String>> deletes = new ConcurrentHashMap<>();
    private final Map<Bigram, Long> bigramLexicon;
    private final Map<String, Long> unigramLexicon;
    private final StringDistance stringDistance;
    private int maxDictionaryWordLength;

    /**
     * Sum of all counts in the dictionary
     */
    private final long n;

    public SymSpell(Map<String, Long> unigrams, Map<Bigram, Long> bigrams, int distanceThreshold, int prefixLength) {
        this.unigramLexicon = new ConcurrentHashMap<>(unigrams);
        this.bigramLexicon = new ConcurrentHashMap<>(bigrams);
        this.maxDictionaryEditDistance = distanceThreshold;
        this.prefixLength = prefixLength;
        this.stringDistance = new DamerauLevenshteinOSA();
        this.n = unigramLexicon.values().stream().reduce(Long::sum).orElse(0L);
        this.unigramLexicon.keySet().parallelStream().forEach(word ->{
            Map<String, Collection<String>> edits = generateEdits(word);
            edits.forEach((string, suggestions) -> this.deletes.computeIfAbsent(string, ignored -> new ArrayList<>()).addAll(suggestions));
        });
        this.maxDictionaryWordLength = this.unigramLexicon.keySet().stream().map(String::length).max(Integer::compareTo).orElse(0);
    }

    private boolean deleteSuggestionPrefix(String delete, int deleteLen, String suggestion, int suggestionLen) {
        if (deleteLen == 0) return true;

        int adjustedSuggestionLen = Math.min(prefixLength, suggestionLen);

        int j = 0;

        for (int i = 0; i < deleteLen; i++) {
            char delChar = delete.charAt(i);
            while (j < adjustedSuggestionLen && delChar != suggestion.charAt(j)) {
                j++;
            }
            if (j == adjustedSuggestionLen) return false;
        }
        return true;
    }

    Set<String> edits(String word, int editDistance, Set<String> deleteWords) {
        editDistance++;
        if (word.length() > 1 && editDistance <= maxDictionaryEditDistance) {
            for (int i = 0; i < word.length(); i++) {
                StringBuilder editableWord = new StringBuilder(word);
                String delete = editableWord.deleteCharAt(i).toString();
                if (deleteWords.add(delete) && editDistance < maxDictionaryEditDistance) {
                    edits(delete, editDistance, deleteWords);
                }
            }
        }
        return deleteWords;
    }

    private Map<String, Collection<String>> generateEdits(String key) {
        Set<String> edits = editsPrefix(key);
        Map<String, Collection<String>> generatedDeletes = new HashMap<>();
        edits.forEach(delete -> generatedDeletes.computeIfAbsent(delete, ignored -> new ArrayList<>()).add(key));
        return generatedDeletes;
    }

    private Set<String> editsPrefix(String key) {
        Set<String> set = new HashSet<>();
        if (key.length() <= maxDictionaryEditDistance) {
            set.add("");
        }
        if (key.length() > prefixLength) {
            key = key.substring(0, prefixLength);
        }
        set.add(key);
        return edits(key, 0, set);
    }

    public List<SuggestItem> lookup(String input, Verbosity verbosity, boolean includeUnknown) throws RuntimeException {
        return lookup(input, verbosity, this.maxDictionaryEditDistance, includeUnknown);
    }

    public List<SuggestItem> lookup(String input, Verbosity verbosity) throws RuntimeException {
        return lookup(input, verbosity, false);
    }

    private List<SuggestItem> lookup(String input, Verbosity verbosity, int maxEditDistance, boolean includeUnknown) throws RuntimeException {
        if (maxEditDistance > maxDictionaryEditDistance) {
            throw new IllegalArgumentException("maxEditDistance > maxDictionaryEditDistance");
        }

        if (unigramLexicon.isEmpty()) {
            throw new RuntimeException("There are no words in the lexicon.");
        }

        List<SuggestItem> suggestions = new ArrayList<>();
        int inputLen = input.length();
        boolean wordIsTooLong = inputLen - maxEditDistance > maxDictionaryWordLength;
        if (wordIsTooLong && includeUnknown) {
            return Arrays.asList(new SuggestItem(input, maxEditDistance + 1, 0));
        }

        if (unigramLexicon.containsKey(input)) {
            SuggestItem suggestSameWord = new SuggestItem(input, 0, unigramLexicon.get(input));
            suggestions.add(suggestSameWord);

            if (!verbosity.equals(ALL)) {
                return suggestions;
            }
        }

        if (maxEditDistance == 0 && includeUnknown && suggestions.isEmpty()) {
            return Arrays.asList(new SuggestItem(input, maxEditDistance + 1, 0));
        }

        Set<String> deletesAlreadyConsidered = new HashSet<>();
        List<String> candidates = new ArrayList<>();

        int inputPrefixLen;
        if (inputLen > prefixLength) {
            inputPrefixLen = prefixLength;
            candidates.add(input.substring(0, inputPrefixLen));
        } else {
            inputPrefixLen = inputLen;
        }
        candidates.add(input);

        Set<String> suggestionsAlreadyConsidered = new HashSet<>();
        suggestionsAlreadyConsidered.add(input);
        int maxEditDistance2 = maxEditDistance;

        int candidatePointer = 0;
        while (candidatePointer < candidates.size()) {
            String candidate = candidates.get(candidatePointer++);
            int candidateLength = candidate.length();
            int lengthDiffBetweenInputAndCandidate = inputPrefixLen - candidateLength;

            boolean candidateDistanceHigherThanSuggestionDistance = lengthDiffBetweenInputAndCandidate > maxEditDistance2;
            if (candidateDistanceHigherThanSuggestionDistance) {
                if (verbosity.equals(ALL)) {
                    continue;
                } else {
                    break;
                }
            }

            if (lengthDiffBetweenInputAndCandidate < maxEditDistance && candidateLength <= prefixLength) {
                if (!verbosity.equals(ALL) && lengthDiffBetweenInputAndCandidate >= maxEditDistance2) {
                    continue;
                }
                candidates.addAll(generateNewCandidates(candidate, deletesAlreadyConsidered));
            }

            Collection<String> preCalculatedDeletes = deletes.get(candidate);
            if (preCalculatedDeletes != null) {
                for (String preCalculatedDelete : preCalculatedDeletes) {
                    if (preCalculatedDelete.equals(input) || ((Math.abs(preCalculatedDelete.length() - inputLen) > maxEditDistance2)
                            || (preCalculatedDelete.length() < candidateLength)
                            || (preCalculatedDelete.length() == candidateLength && !preCalculatedDelete.equals(candidate))) || (Math.min(preCalculatedDelete.length(), prefixLength) > inputPrefixLen
                            && (Math.min(preCalculatedDelete.length(), prefixLength) - candidateLength) > maxEditDistance2)) {
                        continue;
                    }

                    int distance;
                    if (candidateLength == 0) {
                        distance = Math.max(inputLen, preCalculatedDelete.length());
                        if (distance <= maxEditDistance2) {
                            suggestionsAlreadyConsidered.add(preCalculatedDelete);
                        }
                    } else if (preCalculatedDelete.length() == 1) {
                        if (input.contains(preCalculatedDelete)) {
                            distance = inputLen - 1;
                        } else {
                            distance = inputLen;
                        }
                        if (distance <= maxEditDistance2) {
                            suggestionsAlreadyConsidered.add(preCalculatedDelete);
                        }
                    } else {
                        int minDistance = Math.min(inputLen, preCalculatedDelete.length()) - prefixLength;

                        boolean noDistanceCalculationIsRequired = prefixLength - maxEditDistance == candidateLength
                                && (minDistance > 1 && (!input.substring(inputLen + 1 - minDistance).equals(preCalculatedDelete.substring(preCalculatedDelete.length() + 1 - minDistance))))
                                || (minDistance > 0
                                    && input.charAt(inputLen - minDistance) != preCalculatedDelete.charAt(preCalculatedDelete.length() - minDistance)
                                    && input.charAt(inputLen - minDistance - 1) != preCalculatedDelete.charAt(preCalculatedDelete.length() - minDistance)
                                    && input.charAt(inputLen - minDistance) != preCalculatedDelete.charAt(preCalculatedDelete.length() - minDistance - 1));

                        if (noDistanceCalculationIsRequired) {
                            continue;
                        } else {
                            if (!verbosity.equals(ALL)
                                    && !deleteSuggestionPrefix(candidate, candidateLength, preCalculatedDelete, preCalculatedDelete.length())
                                    || !suggestionsAlreadyConsidered.add(preCalculatedDelete)) {
                                continue;
                            }
                            distance = stringDistance.distanceWithEarlyStop(input, preCalculatedDelete, maxEditDistance2);
                            if (distance < 0) {
                                continue;
                            }
                        }

                        if (distance <= maxEditDistance2) {
                            SuggestItem suggestItem = new SuggestItem(preCalculatedDelete, distance, unigramLexicon.get(preCalculatedDelete));
                            if (!suggestions.isEmpty()) {
                                if (verbosity.equals(CLOSEST) && distance < maxEditDistance2) {
                                    suggestions.clear();
                                } else if (verbosity.equals(TOP) && (distance < maxEditDistance2 || suggestItem.getFrequencyOfSuggestionInDict() > suggestions.get(0).getFrequencyOfSuggestionInDict())) {
                                    maxEditDistance2 = distance;
                                    suggestions.add(suggestItem);
                                }
                            }
                            if (!verbosity.equals(ALL)) {
                                maxEditDistance2 = distance;
                            }
                            suggestions.add(suggestItem);
                        }
                    }
                }
            }
        }
        if (suggestions.size() > 1) {
            Collections.sort(suggestions);
        }
        if (includeUnknown && (suggestions.isEmpty())) {
            SuggestItem noSuggestionsFound = new SuggestItem(input, maxEditDistance + 1, 0);
            suggestions.add(noSuggestionsFound);
        }
        return suggestions;
    }

    private Set<String> generateNewCandidates(String candidate, Set<String> deletesAlreadyConsidered) {
        Set<String> newDeletes = new HashSet<>();
        for (int i = 0; i < candidate.length(); i++) {
            StringBuilder editableString = new StringBuilder(candidate);
            String delete = editableString.deleteCharAt(i).toString();
            if (deletesAlreadyConsidered.add(delete)){
                newDeletes.add(delete);
            }
        }
        return newDeletes;
    }

    public List<SuggestItem> lookupCompound(String input, int editDistanceMax, boolean includeUnknown) throws RuntimeException {
        String[] termList = input.split(" ");
        List<SuggestItem> suggestionParts = new ArrayList<>();

        boolean lastCombination = false;

        for (int i = 0; i < termList.length; i++) {
            String currentToken = termList[i];
            List<SuggestItem> suggestionsForCurrentToken = lookup(currentToken, TOP, editDistanceMax, includeUnknown);

            if (i > 0 && !lastCombination) {
                SuggestItem bestSuggestion = suggestionParts.get(suggestionParts.size() - 1);
                Optional<SuggestItem> newSuggestion = combineWords(editDistanceMax, includeUnknown, currentToken, termList[i - 1], bestSuggestion, suggestionsForCurrentToken.isEmpty() ? null : suggestionsForCurrentToken.get(0));

                if (newSuggestion.isPresent()) {
                    suggestionParts.set(suggestionParts.size() - 1, newSuggestion.get());
                    lastCombination = true;
                    continue;
                }
            }

            lastCombination = false;

            if (!suggestionsForCurrentToken.isEmpty()) {
                boolean firstSuggestionIsPerfect = suggestionsForCurrentToken.get(0).getEditDistance() == 0;
                if (firstSuggestionIsPerfect || currentToken.length() == 1) {
                    suggestionParts.add(suggestionsForCurrentToken.get(0));
                } else {
                    splitWords(editDistanceMax, termList, suggestionsForCurrentToken, suggestionParts, i);
                }
            } else {
                splitWords(editDistanceMax, termList, suggestionsForCurrentToken, suggestionParts, i);
            }
        }
        double freq = n;
        StringBuilder stringBuilder = new StringBuilder();
        for (SuggestItem suggestItem : suggestionParts) {
            stringBuilder.append(suggestItem.getSuggestion()).append(" ");
            freq *= suggestItem.getFrequencyOfSuggestionInDict() / n;
        }

        String term = stringBuilder.toString().replaceFirst("\\s++$", ""); // this replace call trims all trailing whitespace
        SuggestItem suggestion = new SuggestItem(term, stringDistance.distanceWithEarlyStop(input, term, Integer.MAX_VALUE), freq);
        List<SuggestItem> suggestionsLine = new ArrayList<>();
        suggestionsLine.add(suggestion);
        return suggestionsLine;
    }

    private void splitWords(int editDistanceMax, String[] termList, List<SuggestItem> suggestions, List<SuggestItem> suggestionParts, int i) throws RuntimeException {
        SuggestItem suggestionSplitBest = null;
        if (!suggestions.isEmpty()) suggestionSplitBest = suggestions.get(0);

        String word = termList[i];
        if (word.length() > 1) {
            for (int j = 1; j < word.length(); j++) {
                String part1 = word.substring(0, j);
                String part2 = word.substring(j);
                SuggestItem suggestionSplit;
                List<SuggestItem> suggestions1 = lookup(part1, TOP, editDistanceMax, false);
                if (!suggestions1.isEmpty()) {
                    List<SuggestItem> suggestions2 = lookup(part2, TOP, editDistanceMax, false);
                    if (!suggestions2.isEmpty()) {

                        Bigram splitTerm = new Bigram(suggestions1.get(0).getSuggestion(), suggestions2.get(0).getSuggestion());
                        int splitDistance = stringDistance.distanceWithEarlyStop(word, splitTerm.toString(), editDistanceMax);

                        if (splitDistance < 0) splitDistance = editDistanceMax + 1;

                        if (suggestionSplitBest != null) {
                            if (splitDistance > suggestionSplitBest.getEditDistance()) continue;
                            if (splitDistance < suggestionSplitBest.getEditDistance()) suggestionSplitBest = null;
                        }
                        double freq;
                        if (bigramLexicon.containsKey(splitTerm)) {
                            freq = bigramLexicon.get(splitTerm);

                            if (!suggestions.isEmpty()) {
                                if ((suggestions1.get(0).getSuggestion() + suggestions2.get(0).getSuggestion()).equals(word)) {
                                    freq = Math.max(freq, suggestions.get(0).getFrequencyOfSuggestionInDict() + 2);
                                } else if ((suggestions1.get(0)
                                                        .getSuggestion()
                                                        .equals(suggestions.get(0).getSuggestion())
                                        || suggestions2.get(0)
                                                       .getSuggestion()
                                                       .equals(suggestions.get(0).getSuggestion()))) {
                                    freq = Math.max(freq, suggestions.get(0).getFrequencyOfSuggestionInDict() + 1);
                                }

                            } else if ((suggestions1.get(0).getSuggestion() + suggestions2.get(0).getSuggestion()).equals(word)) {
                                freq = Math.max(freq, Math.max(suggestions1.get(0).getFrequencyOfSuggestionInDict(), suggestions2.get(0).getFrequencyOfSuggestionInDict()));
                            }
                        } else {
                            // The Naive Bayes probability of the word combination is the product of the two
                            // word probabilities: P(AB) = P(A) * P(B)
                            // use it to estimate the frequency count of the combination, which then is used
                            // to rank/select the best splitting variant
                            freq = Math.min(BIGRAM_COUNT_MIN, getNaiveBayesProbOfCombination(suggestions1, suggestions2));
                        }
                        suggestionSplit = new SuggestItem(splitTerm.toString(), splitDistance, freq);

                        if (suggestionSplitBest == null || suggestionSplit.getFrequencyOfSuggestionInDict() > suggestionSplitBest.getFrequencyOfSuggestionInDict()){
                            suggestionSplitBest = suggestionSplit;
                        }
                    }
                }
            }
            if (suggestionSplitBest != null) {
                suggestionParts.add(suggestionSplitBest);
            } else {
                SuggestItem suggestItem = new SuggestItem(word, editDistanceMax + 1, estimatedWordOccurrenceProbability(word)); // estimated word occurrence probability P=10 / (N * 10^word length l)

                suggestionParts.add(suggestItem);
            }
        } else {
            SuggestItem suggestItem = new SuggestItem(word, editDistanceMax + 1, estimatedWordOccurrenceProbability(word));
            suggestionParts.add(suggestItem);
        }
    }

    private long getNaiveBayesProbOfCombination(List<SuggestItem> suggestions1, List<SuggestItem> suggestions2) {
        return (long) ((suggestions1.get(0).getFrequencyOfSuggestionInDict() / (double) n) * suggestions2.get(0).getFrequencyOfSuggestionInDict());
    }

    private long estimatedWordOccurrenceProbability(String word) {
        return (long) ((double) 10 / Math.pow(10, word.length()));
    }

    Optional<SuggestItem> combineWords(int editDistanceMax, boolean includeUnknown, String token, String previousToken, SuggestItem suggestItem, SuggestItem secondBestSuggestion) throws RuntimeException {
        List<SuggestItem> suggestionsCombination = lookup(previousToken + token, TOP, editDistanceMax, includeUnknown);
        if (!suggestionsCombination.isEmpty()) {
            SuggestItem best2;
            // TODO fixme
            best2 = Optional.ofNullable(secondBestSuggestion).orElseGet(() -> new SuggestItem(token, editDistanceMax + 1, estimatedWordOccurrenceProbability(token)));

            int distance = suggestItem.getEditDistance() + best2.getEditDistance();

            SuggestItem firstSuggestion = suggestionsCombination.get(0);

            if (distance >= 0 && (firstSuggestion.getEditDistance() + 1 < distance)
                    || (firstSuggestion.getEditDistance() + 1 == distance
                    && firstSuggestion.getFrequencyOfSuggestionInDict()
                    > suggestItem.getFrequencyOfSuggestionInDict() / n
                    * best2.getFrequencyOfSuggestionInDict())) {

                return Optional.of(new SuggestItem(
                        firstSuggestion.getSuggestion(),
                        firstSuggestion.getEditDistance(),
                        firstSuggestion.getFrequencyOfSuggestionInDict()));
            }
        }
        return Optional.empty();
    }

    public Map<String, Long> getUnigramLexicon() {
        return unigramLexicon;
    }

    public Map<Bigram, Long> getBigramLexicon() {
        return bigramLexicon;
    }

    public Map<String, Collection<String>> getDeletes() {
        return deletes;
    }

    public int getMaxDictionaryEditDistance() {
        return maxDictionaryEditDistance;
    }

    public void addBigrams(Map<Bigram, Long> bigrams) {
        bigrams.forEach((key, value) -> bigramLexicon.merge(key, value, (v1, v2) -> v1 + v2));
    }

    public void addUnigrams(Map<String, Long> unigrams) {
        unigrams.forEach((key, value) -> unigramLexicon.merge(key, value, (v1, v2) -> v1 + v2));
        unigrams.keySet().parallelStream().forEach(word ->{
                Map<String, Collection<String>> edits = generateEdits(word);
                edits.forEach((string, suggestions) -> deletes.merge(string, suggestions, (v1, v2) -> Stream.concat(v1.stream(), v2.stream()).distinct().collect(Collectors.toList())));
            });
        int myMaxDictionaryWordLength = unigrams.keySet().stream().map(String::length).max(Integer::compareTo).orElse(0);
        maxDictionaryWordLength = (myMaxDictionaryWordLength > maxDictionaryWordLength) ? myMaxDictionaryWordLength : maxDictionaryWordLength;
    }

}
