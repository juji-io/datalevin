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

package datalevin.sm;

import java.util.Objects;

/**
 * Holds a pair of words.
 */
public class Bigram {
    private final String word1;
    private final String word2;

    /**
     * Constructs a bigram with the specified words.
     * @param word1 first word
     * @param word2 second word
     */
    public Bigram(String word1, String word2) {
        this.word1 = word1;
        this.word2 = word2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bigram)) return false;
        Bigram bigram = (Bigram) o;
        return Objects.equals(word1, bigram.word1) &&
            Objects.equals(word2, bigram.word2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word1, word2);
    }

    @Override
    public String toString() {
        return word1 + ' ' + word2;
    }
}
