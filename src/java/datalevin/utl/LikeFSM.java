package datalevin.utl;

public class LikeFSM {

    static final int ALPH_SIZE = 256;
    static final byte S_WILD_BYTE = (byte)95; // _
    static final byte M_WILD_BYTE = (byte)37; // %

    byte[] pat;
    int M;
    int[][] TF;

    public LikeFSM(byte[] pattern) {
        pat = pattern;
        M = pattern.length;
        TF = new int[M + 1][ALPH_SIZE];
        compile();
    }

    void compile() {
        int i, lps = 0, x;
        boolean isMWild = false;

        for (i = 0; i < M; i++) {
            byte iByte = pat[i];
            int iInt = iByte & 0xFF;
            int next = i + 1;

            switch (iByte) {
            case M_WILD_BYTE:
                int nnext = next + 1;
                for (x = 0; x < ALPH_SIZE; x++)
                    if ((next < M) && ((byte)x == pat[next])) TF[i][x] = nnext;
                    else TF[i][x] = next;
                isMWild = true;
                break;
            case S_WILD_BYTE:
                for (x = 0; x < ALPH_SIZE; x++) TF[i][x] = next;
                isMWild = false;
                break;
            default:
                if (i == 0)
                    for (x = 0; x < ALPH_SIZE; x++) TF[i][x] = 0;
                else if (isMWild == true)
                    for (x = 0; x < ALPH_SIZE; x++) TF[i][x] = i;
                else
                    for (x = 0; x < ALPH_SIZE; x++) TF[i][x] = TF[lps][x];
                TF[i][iInt] = next;
                isMWild = false;
            }

            if (i < M) lps = TF[lps][iInt];
        }
    }

    public boolean match(byte[] txt) {
        int iStart, j;
        byte sByte = pat[0];

        if ((sByte == M_WILD_BYTE) || (sByte == S_WILD_BYTE)) {
            iStart = 0;
            j = 0;
        } else if (sByte == txt[0]) {
            iStart = 1;
            j = 1;
        } else return false;

        int pLast = M - 1;
        byte eByte = pat[pLast];
        int N = txt.length;

        for (int i = iStart; i < N; i++) {
            j = TF[j][txt[i] & 0xFF];

            if (j == M) {
                int tLast = N - 1;

                switch (eByte) {
                case M_WILD_BYTE: return true;
                case S_WILD_BYTE:
                    if (i == tLast) return true;
                    else return false;
                default:
                    if (eByte == txt[tLast]) return true;
                    else return false;
                }
            }
        }

        if ((eByte == M_WILD_BYTE) && (j == pLast)) return true;

        return false;
    }
}
