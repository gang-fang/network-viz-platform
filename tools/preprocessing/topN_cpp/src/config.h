#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>
#include <cstddef>
#include <cctype>
#include <sstream>
#include <cmath>

// Program-wide constants
constexpr int MAX_SEQUENCE_NAME = 64;
constexpr int DEFAULT_KMER_SIZE = 2;
constexpr int DEFAULT_TOP_N = 50;
constexpr float DEFAULT_PREFILTER_SAFETY_MARGIN = 1.5f;
constexpr float DEFAULT_PREFILTER_TIE_CAP = 1.6f;
constexpr float DEFAULT_MIN_HIT_SCORE = 27.0f; // percent threshold for NW hits
constexpr float DEFAULT_LENGTH_RATIO = 1.5f;
constexpr float DEFAULT_LOWER_THRESHOLD = 0.05f;

// Core data structures
struct Sequence {
    std::string name;
    std::string sequence;
    
    Sequence() = default;
    Sequence(const std::string& n, const std::string& s)
        : name(n), sequence(s) {}
};

// Program configuration
struct ProgramConfig {
    // Input/Output
    std::string queryFile;
    std::string targetFile;
    std::string outputFile;
    std::string outputFormat = "c"; // Output format: "j" (JSON), "m" (Markdown), "c" (CSV)
    
    // FASTP parameters
    int kmerSize = DEFAULT_KMER_SIZE;
    float lengthRatio = DEFAULT_LENGTH_RATIO;
    float lowerThreshold = DEFAULT_LOWER_THRESHOLD;
    
    // Alignment parameters
    int gapOpenPenalty = 120;
    int gapExtendPenalty = 80;
    std::string scoringMatrixFile;
    
    // Results parameters
    int topN = DEFAULT_TOP_N;
    float prefilterSafetyMargin = DEFAULT_PREFILTER_SAFETY_MARGIN;
    float prefilterTieCap = DEFAULT_PREFILTER_TIE_CAP;
    float minHitScore = DEFAULT_MIN_HIT_SCORE; // minimum NW similarity (%)
    
    // Program behavior
    bool verbose = false;
    int numThreads = 1;

    int targetPrefilterCount() const {
        if (topN <= 0) return topN;
        return std::max(topN,
                        static_cast<int>(std::ceil(static_cast<double>(topN) *
                                                   static_cast<double>(prefilterSafetyMargin))));
    }

    int maxPrefilterCount() const {
        if (topN <= 0) return topN;
        return std::max(targetPrefilterCount(),
                        static_cast<int>(std::ceil(static_cast<double>(topN) *
                                                   static_cast<double>(prefilterTieCap))));
    }
};

// Default BLOSUM62 matrix as a string
inline const std::string& getDefaultBLOSUM62Matrix() {
    static const std::string BLOSUM62 = R"(#  Matrix made by matblas from blosum62.iij
#  * column uses minimum score
#  BLOSUM Clustered Scoring Matrix in 1/2 Bit Units
#  Blocks Database = /data/blocks_5.0/blocks.dat
#  Cluster Percentage: >= 62
#  Entropy =   0.6979, Expected =  -0.5209
 A  R  N  D  C  Q  E  G  H  I  L  K  M  F  P  S  T  W  Y  V  B  Z  X  *
 4 -1 -2 -2  0 -1 -1  0 -2 -1 -1 -1 -1 -2 -1  1  0 -3 -2  0 -2 -1  0 -4 
-1  5  0 -2 -3  1  0 -2  0 -3 -2  2 -1 -3 -2 -1 -1 -3 -2 -3 -1  0 -1 -4 
-2  0  6  1 -3  0  0  0  1 -3 -3  0 -2 -3 -2  1  0 -4 -2 -3  3  0 -1 -4 
-2 -2  1  6 -3  0  2 -1 -1 -3 -4 -1 -3 -3 -1  0 -1 -4 -3 -3  4  1 -1 -4 
 0 -3 -3 -3  9 -3 -4 -3 -3 -1 -1 -3 -1 -2 -3 -1 -1 -2 -2 -1 -3 -3 -2 -4 
-1  1  0  0 -3  5  2 -2  0 -3 -2  1  0 -3 -1  0 -1 -2 -1 -2  0  3 -1 -4 
-1  0  0  2 -4  2  5 -2  0 -3 -3  1 -2 -3 -1  0 -1 -3 -2 -2  1  4 -1 -4 
 0 -2  0 -1 -3 -2 -2  6 -2 -4 -4 -2 -3 -3 -2  0 -2 -2 -3 -3 -1 -2 -1 -4 
-2  0  1 -1 -3  0  0 -2  8 -3 -3 -1 -2 -1 -2 -1 -2 -2  2 -3  0  0 -1 -4 
-1 -3 -3 -3 -1 -3 -3 -4 -3  4  2 -3  1  0 -3 -2 -1 -3 -1  3 -3 -3 -1 -4 
-1 -2 -3 -4 -1 -2 -3 -4 -3  2  4 -2  2  0 -3 -2 -1 -2 -1  1 -4 -3 -1 -4 
-1  2  0 -1 -3  1  1 -2 -1 -3 -2  5 -1 -3 -1  0 -1 -3 -2 -2  0  1 -1 -4 
-1 -1 -2 -3 -1  0 -2 -3 -2  1  2 -1  5  0 -2 -1 -1 -1 -1  1 -3 -1 -1 -4 
-2 -3 -3 -3 -2 -3 -3 -3 -1  0  0 -3  0  6 -4 -2 -2  1  3 -1 -3 -3 -1 -4 
-1 -2 -2 -1 -3 -1 -1 -2 -2 -3 -3 -1 -2 -4  7 -1 -1 -4 -3 -2 -2 -1 -2 -4 
 1 -1  1  0 -1  0  0  0 -1 -2 -2  0 -1 -2 -1  4  1 -3 -2 -2  0  0  0 -4 
 0 -1  0 -1 -1 -1 -1 -2 -2 -1 -1 -1 -1 -2 -1  1  5 -2 -2  0 -1 -1  0 -4 
-3 -3 -4 -4 -2 -2 -3 -2 -2 -3 -2 -3 -1  1 -4 -3 -2 11  2 -3 -4 -3 -2 -4 
-2 -2 -2 -3 -2 -1 -2 -3  2 -1 -1 -2 -1  3 -3 -2 -2  2  7 -1 -3 -2 -1 -4 
 0 -3 -3 -3 -1 -2 -2 -3 -3  3  1 -2  1 -1 -2 -2  0 -3 -1  4 -3 -2 -1 -4 
-2 -1  3  4 -3  0  1 -1  0 -3 -4  0 -3 -3 -2  0 -1 -4 -3 -3  4  1 -1 -4 
-1  0  0  1 -3  3  4 -2  0 -3 -3  1 -1 -3 -1  0 -1 -3 -2 -2  1  4 -1 -4 
 0 -1 -1 -1 -2 -1 -1 -1 -1 -1 -1 -1 -1 -1 -2  0  0 -2 -1 -1 -1 -1 -1 -4 
-4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4 -4  1)";
    return BLOSUM62;
}

// Function to parse a substitution matrix from a stream
inline bool parseSubstitutionMatrix(std::istream& stream, std::vector<std::vector<int>>& matrix, std::vector<char>& aminoAcids) {
    std::string line;
    bool foundHeader = false;
    auto isNumericToken = [](const std::string& token) {
        if (token.empty()) return false;
        size_t start = (token[0] == '-' || token[0] == '+') ? 1 : 0;
        if (start >= token.size()) return false;
        for (size_t i = start; i < token.size(); ++i) {
            if (!std::isdigit(static_cast<unsigned char>(token[i]))) return false;
        }
        return true;
    };
    
    // Skip comment lines and find the header line with amino acid symbols
    while (std::getline(stream, line)) {
        if (line.empty() || line[0] == '#') continue;
        
        // Parse amino acid symbols from the header line
        std::istringstream iss(line);
        aminoAcids.clear();
        char aa;
        while (iss >> aa) {
            aminoAcids.push_back(aa);
        }
        
        foundHeader = true;
        break;
    }
    
    if (!foundHeader || aminoAcids.empty()) return false;
    
    // Initialize matrix with the correct size
    matrix.resize(aminoAcids.size(), std::vector<int>(aminoAcids.size(), 0));
    
    // Parse matrix values
    size_t row = 0;
    while (std::getline(stream, line) && row < aminoAcids.size()) {
        if (line.empty() || line[0] == '#') continue;

        std::istringstream rowStream(line);
        std::string token;
        bool firstToken = true;
        size_t col = 0;
        while (rowStream >> token && col < aminoAcids.size()) {
            if (firstToken && !isNumericToken(token)) {
                // Token is a row label, skip it
                firstToken = false;
                continue;
            }
            firstToken = false;
            try {
                matrix[row][col] = std::stoi(token);
            } catch (const std::exception&) {
                return false;
            }
            col++;
        }

        if (col != aminoAcids.size()) return false;
        row++;
    }
    
    return row == aminoAcids.size();
}

#endif // CONFIG_H 
