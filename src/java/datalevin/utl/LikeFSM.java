package datalevin.utl;

public class LikeFSM {

    static final int ALPH_SIZE = 256;
    static final byte S_WILD_BYTE = (byte)95; // _
    static final byte M_WILD_BYTE = (byte)37; // %
    static final byte DEFAULT_ESC = (byte)33; // !

    static final byte ESC_S_WILD = (byte)-64;
    static final byte ESC_M_WILD = (byte)-63;
    static final byte ESC_ESC = (byte)-11;

    byte esc;

    byte[] pat;
    int M;
    int[][] TF;

    public LikeFSM(byte[] pattern, char escapeCharacter) {
        esc = (byte)escapeCharacter;
        init(pattern);
    }

    public LikeFSM(byte[] pattern) {
        esc = DEFAULT_ESC;
        init(pattern);
    }

    public static boolean isValid(byte[] pattern, char escape) {
        boolean inESC = false;
        int n = pattern.length;

        for (int i = 0; i < n; i++) {
            byte cur = pattern[i];
            if (cur == escape) {
                if (inESC) inESC = false;
                else inESC = true;
            } else {
                switch (cur) {
                case S_WILD_BYTE:
                    if (inESC) inESC = false;
                    break;
                case M_WILD_BYTE:
                    if (inESC) inESC = false;
                    break;
                default:
                    if (inESC) {
                        throw new IllegalStateException("Can only escape %, _, and escape character itself in like pattern");
                    }
                }
            }
        }
        return true;
    }


    void init(byte[] pattern) {
        pat = escape(pattern);
        M = pat.length;
        TF = new int[M + 1][ALPH_SIZE];
        compile();
    }

    byte[] escape(byte[] pattern) {
        int n = pattern.length;
        byte[] dst = new byte[n];

        int i, j = 0;
        boolean inESC = false;

        for (i = 0; i < n; i++) {
            byte cur = pattern[i];
            if (cur == esc) {
                if (inESC) {
                    dst[j++] = ESC_ESC;
                    inESC = false;
                } else inESC = true;
            } else {
                switch (cur) {
                case S_WILD_BYTE:
                    if (inESC) {
                        dst[j++] = ESC_S_WILD;
                        inESC = false;
                    } else dst[j++] = cur;
                    break;
                case M_WILD_BYTE:
                    if (inESC) {
                        dst[j++] = ESC_M_WILD;
                        inESC = false;
                    } else dst[j++] = cur;
                    break;
                default:
                    if (inESC) {
                        throw new IllegalStateException("Can only escape %, _, and escape character itself in like pattern");
                    } else dst[j++] = cur;
                }
            }
        }

        byte[] ret = new byte[j];
        System.arraycopy(dst, 0, ret, 0, j);
        return ret;
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

                switch (iByte) {
                case ESC_ESC:
                    TF[i][esc & 0xFF] = next;
                    break;
                case ESC_M_WILD:
                    TF[i][M_WILD_BYTE & 0xFF] = next;
                    break;
                case ESC_S_WILD:
                    TF[i][S_WILD_BYTE & 0xFF] = next;
                    break;
                default:
                    TF[i][iInt] = next;
                }

                isMWild = false;
            }

            if (i < M) lps = TF[lps][iInt];
        }
    }

    public boolean match(byte[] txt) {

        int pLast = M - 1;
        byte eByte = pat[pLast];
        byte sByte = pat[0];

        int N = txt.length;
        int i = 0;
        int j = 0;

        while (i < N) {

            j = TF[j][txt[i] & 0xFF];

            if (j == 0) {
                if (sByte != M_WILD_BYTE) return false;
                else continue;
            }

            if ((j == M) && ((eByte == M_WILD_BYTE) || (i == N - 1)))
                return true;

            i++;
        }

        if ((j == pLast) && (eByte == M_WILD_BYTE)) return true;

        return false;
    }
}
