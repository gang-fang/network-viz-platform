#include "matrix.h"
#include <fstream>
#include <iostream>

bool loadSubstitutionMatrix(const std::string &filename,
                            std::vector<std::vector<int>> &matrix,
                            std::vector<char> &aminoAcids) {
    std::ifstream infile(filename);
    if (!infile) {
        std::cerr << "Error: Cannot open substitution matrix file: " << filename << std::endl;
        return false;
    }
    return parseSubstitutionMatrix(infile, matrix, aminoAcids);
}
