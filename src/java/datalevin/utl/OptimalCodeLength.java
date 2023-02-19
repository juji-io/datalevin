package datalevin.utl;

import  org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

public class OptimalCodeLength {

    public static int [] generate(final long [] freqs) {
        int n = freqs.length;

        int [] L = new int[n];

        long maxp = 1;
        long [] P = new long[n];

        for (int k = 0; k < n; k++) {
            L[k] = 0;

            long fk = freqs[k];
            P[k] = fk;
            maxp += fk;
        }

        IntArrayList s = new IntArrayList();
        IntArrayList d = new IntArrayList();

        for (int m = 0; m < n - 1; m++) {
            int i = 0;
            int i1 = 0;
            int i2 = 0;
            long pmin = maxp;
            int sumL = -1;

            while (i < n - 1) {
                if (P[i] == 0) {
                    i++;
                    continue;
                }

                int j1 = i;
                int j2 = -1;
                long min1 = P[i];
                long min2 = maxp;
                int minL1 = L[i];
                int minL2 = -1;

                int j = 0;
                for (j = i + 1; j < n; j++) {
                    long pj = P[j];
                    int lj = L[j];

                    if (pj == 0) continue;
                    if (pj < min1 || (pj == min1 && lj < minL1)) {
                        min2 = min1;
                        j2 = j1;
                        minL2 = minL1;
                        min1 = pj;
                        j1 = j;
                        minL1 = lj;
                    } else if (pj < min2 || (pj == min2 && lj < minL2)) {
                        min2 = pj;
                        j2 = j;
                        minL2 = lj;
                    }
                    if (lj == 0) break;
                }

                long pt = P[j1] + P[j2];
                int sumLt = L[j1] + L[j2];
                if (pt < pmin || (pt == pmin && sumLt < sumL)) {
                    pmin = pt;
                    sumL = sumLt;
                    i1 = j1;
                    i2 = j2;
                }
                i = j;
            }

            if (i1 > i2) {
                int tmp = i1;
                i1 = i2;
                i2 = tmp;
            }
            s.add(i1);
            d.add(i2);
            P[i1] = pmin;
            P[i2] = 0;
            L[i1] = sumL + 1;
        }

        int n2 = n - 2;
        L[s.get(n2)] = 0;
        for (int m = n2; m >= 0; m--) {
            int sm = s.get(m);
            L[sm] += 1;
            L[d.get(m)] = L[sm];
        }

        return L;
    }
}
