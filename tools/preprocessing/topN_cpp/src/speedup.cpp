#include "speedup.h"
#include <cctype>

// Define the global substitution matrix
int substitutionMatrix[26][26];
int substitutionWeightMatrix[26][26];

// Precompute substitution scores for all amino acid pairs
void precomputeSubstitutionScores(const std::vector<std::vector<int>>& matrix,
                                  const std::vector<char>& aminoAcids) {
    for (int i = 0; i < 26; ++i) {
        for (int j = 0; j < 26; ++j) {
            substitutionMatrix[i][j] = 0;
            substitutionWeightMatrix[i][j] = 100;
        }
    }

    int smin = 0;
    int smax = 0;
    bool init = false;
    for (size_t i = 0; i < aminoAcids.size(); i++) {
        for (size_t j = 0; j < aminoAcids.size(); j++) {
            int score = matrix[i][j];
            if (!init) {
                smin = smax = score;
                init = true;
            } else {
                if (score < smin) smin = score;
                if (score > smax) smax = score;
            }
        }
    }

    const double denom = static_cast<double>((smax > smin) ? (smax - smin) : 1);
    for (size_t i = 0; i < aminoAcids.size(); i++) {
        char a = std::toupper(aminoAcids[i]);
        for (size_t j = 0; j < aminoAcids.size(); j++) {
            char b = std::toupper(aminoAcids[j]);
            if (a >= 'A' && a <= 'Z' && b >= 'A' && b <= 'Z') {
                const int rawScore = matrix[i][j];
                substitutionMatrix[a - 'A'][b - 'A'] = rawScore;

                const int normalizedScore = (a == b) ? smax : rawScore;
                const double sim = static_cast<double>(normalizedScore - smin) / denom;
                substitutionWeightMatrix[a - 'A'][b - 'A'] =
                    100 - static_cast<int>(sim * 100.0 + 0.5);
            }
        }
    }
}
