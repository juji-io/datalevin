/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datalevin.search;

/**
 * Query-specific Unicode Levenshtein DFA using Apache Lucene 9.12.3's
 * generated Schulz/Mihov transition tables. Transitions are evaluated on
 * demand, without materializing an automaton graph or depending on Lucene.
 */
final class LevenshteinAutomata {
  private final int[] word;
  private final int edits;
  private final ParametricDescription description;

  LevenshteinAutomata(String term, int edits, boolean transpositions) {
    if (edits < 0 || edits > 2) {
      throw new IllegalArgumentException("max-edits must be between 0 and 2");
    }
    this.word = term.codePoints().toArray();
    this.edits = edits;
    this.description = switch (edits) {
      case 0 -> null;
      case 1 -> transpositions
          ? new Lev1TParametricDescription(word.length)
          : new Lev1ParametricDescription(word.length);
      default -> transpositions
          ? new Lev2TParametricDescription(word.length)
          : new Lev2ParametricDescription(word.length);
    };
  }

  int step(int state, int codePoint) {
    if (state < 0) return -1;
    if (edits == 0) {
      return state < word.length && word[state] == codePoint ? state + 1 : -1;
    }
    int position = description.getPosition(state);
    int end = Math.min(word.length, position + 2 * edits + 1);
    int vector = 0;
    for (int i = position; i < end; i++) {
      vector = (vector << 1) | (word[i] == codePoint ? 1 : 0);
    }
    return description.transition(state, position, vector);
  }

  boolean isAccept(int state) {
    return state >= 0 && (edits == 0
        ? state == word.length : description.isAccept(state));
  }

  abstract static class ParametricDescription {
    protected final int w;
    protected final int n;
    private final int[] minErrors;

    ParametricDescription(int w, int n, int[] minErrors) {
      this.w = w;
      this.n = n;
      this.minErrors = minErrors;
    }

    /** Return the number of states needed to compute a Levenshtein DFA */
    int size() {
      return minErrors.length * (w + 1);
    }

    /**
     * Returns true if the <code>state</code> in any Levenshtein DFA is an accept state (final
     * state).
     */
    boolean isAccept(int absState) {
      // decode absState -> state, offset
      int state = absState / (w + 1);
      int offset = absState % (w + 1);
      assert offset >= 0;
      return w - offset + minErrors[state] <= n;
    }

    /**
     * Returns the position in the input word for a given <code>state</code>. This is the minimal
     * boundary for the state.
     */
    int getPosition(int absState) {
      return absState % (w + 1);
    }

    /**
     * Returns the state number for a transition from the given <code>state</code>, assuming <code>
     * position</code> and characteristic vector <code>vector</code>
     */
    abstract int transition(int state, int position, int vector);

    private static final long[] MASKS =
        new long[] {
          0x1,
          0x3,
          0x7,
          0xf,
          0x1f,
          0x3f,
          0x7f,
          0xff,
          0x1ff,
          0x3ff,
          0x7ff,
          0xfff,
          0x1fff,
          0x3fff,
          0x7fff,
          0xffff,
          0x1ffff,
          0x3ffff,
          0x7ffff,
          0xfffff,
          0x1fffff,
          0x3fffff,
          0x7fffff,
          0xffffff,
          0x1ffffff,
          0x3ffffff,
          0x7ffffff,
          0xfffffff,
          0x1fffffff,
          0x3fffffff,
          0x7fffffffL,
          0xffffffffL,
          0x1ffffffffL,
          0x3ffffffffL,
          0x7ffffffffL,
          0xfffffffffL,
          0x1fffffffffL,
          0x3fffffffffL,
          0x7fffffffffL,
          0xffffffffffL,
          0x1ffffffffffL,
          0x3ffffffffffL,
          0x7ffffffffffL,
          0xfffffffffffL,
          0x1fffffffffffL,
          0x3fffffffffffL,
          0x7fffffffffffL,
          0xffffffffffffL,
          0x1ffffffffffffL,
          0x3ffffffffffffL,
          0x7ffffffffffffL,
          0xfffffffffffffL,
          0x1fffffffffffffL,
          0x3fffffffffffffL,
          0x7fffffffffffffL,
          0xffffffffffffffL,
          0x1ffffffffffffffL,
          0x3ffffffffffffffL,
          0x7ffffffffffffffL,
          0xfffffffffffffffL,
          0x1fffffffffffffffL,
          0x3fffffffffffffffL,
          0x7fffffffffffffffL
        };

    protected int unpack(long[] data, int index, int bitsPerValue) {
      final long bitLoc = bitsPerValue * (long) index;
      final int dataLoc = (int) (bitLoc >> 6);
      final int bitStart = (int) (bitLoc & 63);
      // System.out.println("index=" + index + " dataLoc=" + dataLoc + " bitStart=" + bitStart + "
      // bitsPerV=" + bitsPerValue);
      if (bitStart + bitsPerValue <= 64) {
        // not split
        return (int) ((data[dataLoc] >> bitStart) & MASKS[bitsPerValue - 1]);
      } else {
        // split
        final int part = 64 - bitStart;
        return (int)
            (((data[dataLoc] >> bitStart) & MASKS[part - 1])
                + ((data[1 + dataLoc] & MASKS[bitsPerValue - part - 1]) << part));
      }
    }
  }
}
