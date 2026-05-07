#ifndef SPEEDUP_H
#define SPEEDUP_H

#include <vector>

// Declare the global substitution matrix (extern to avoid multiple definitions)
extern int substitutionMatrix[26][26];
extern int substitutionWeightMatrix[26][26];

// Function to precompute substitution scores
void precomputeSubstitutionScores(const std::vector<std::vector<int>>& matrix,
                                  const std::vector<char>& aminoAcids);

#endif // SPEEDUP_H
