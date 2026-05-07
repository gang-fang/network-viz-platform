#include "nw_align.h"
#include "speedup.h"
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <climits>

namespace {
    static thread_local std::vector<int> distH;
    static thread_local std::vector<int> distF;
    static thread_local std::vector<int> distE;

    constexpr int DIST_INF = INT_MAX / 4;

    double computeGlobalSimilarity(const std::string& A, const std::string& B,
                                         int open, int ext) {
        const int lenA = static_cast<int>(A.size());
        const int lenB = static_cast<int>(B.size());
        const int cols = lenB + 1;

        if (static_cast<int>(distH.size()) < cols) {
            distH.resize(cols);
            distF.resize(cols);
            distE.resize(cols);
        }

        int* H = distH.data();
        int* F = distF.data();
        int* E = distE.data();

        H[0] = 0;
        E[0] = DIST_INF;
        F[0] = DIST_INF;
        for (int j = 1; j <= lenB; ++j) {
            H[j] = open + (j - 1) * ext;
            E[j] = H[j];
            F[j] = DIST_INF;
        }

        for (int i = 1; i <= lenA; ++i) {
            int prevDiag = H[0];
            H[0] = open + (i - 1) * ext;
            F[0] = H[0];
            E[0] = DIST_INF;

            const unsigned char ai = static_cast<unsigned char>(A[i - 1]);
            int e = DIST_INF;
            for (int j = 1; j <= lenB; ++j) {
                const int up = H[j];
                e = std::min(H[j - 1] + open, e + ext);
                F[j] = std::min(up + open, F[j] + ext);

                const unsigned char bj = static_cast<unsigned char>(B[j - 1]);
                const int sub = substitutionWeightMatrix[ai - 'A'][bj - 'A'];
                H[j] = std::min({prevDiag + sub, e, F[j]});
                prevDiag = up;
            }
        }

        const int norm = std::max(lenA, lenB);
        return norm > 0 ? 100.0 - static_cast<double>(H[lenB]) / static_cast<double>(norm) : 0.0;
    }

    // Normalize sequences before scoring by uppercasing and dropping
    // non-alphabetic characters.
    std::string preprocess(const std::string& seq) {
        std::string result;
        result.reserve(seq.length());
        for (char c : seq)
            if (std::isalpha(c)) result += std::toupper(c);
        return result;
    }

} // anonymous namespace

// Similarity percentage for ranking, using the global distance
// formulation with normalized substitution weights in [0,100], affine gaps
// k0 + (l-1) * k1, and final score 100 - distance / max(lenA, lenB).
double computeNWSimilarityPercentage(
    const Sequence& querySeq, const Sequence& targetSeq,
    const ProgramConfig& config) {
    std::string seqA = preprocess(querySeq.sequence);
    std::string seqB = preprocess(targetSeq.sequence);
    if (seqA.empty() || seqB.empty()) return 0.0;
    return computeGlobalSimilarity(seqA, seqB,
                                         config.gapOpenPenalty,
                                         config.gapExtendPenalty);
}
