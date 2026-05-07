#ifndef MATRIX_H
#define MATRIX_H

#include "config.h"
#include <string>
#include <vector>

// Loads the substitution matrix from a file.
// Returns true on success.
bool loadSubstitutionMatrix(const std::string &filename,
                            std::vector<std::vector<int>> &matrix,
                            std::vector<char> &aminoAcids);

#endif // MATRIX_H 
