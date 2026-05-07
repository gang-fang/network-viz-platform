#include "io_utils.h"
#include <array>
#include <cctype>

namespace io {

namespace {

std::string extractHeaderToken(const std::string& header) {
    const size_t tokenEnd = header.find_first_of(" \t");
    if (tokenEnd == std::string::npos) {
        return header;
    }
    return header.substr(0, tokenEnd);
}

bool isUniProtPrefix(const std::string& prefix) {
    return prefix == "sp" || prefix == "tr";
}

std::string normalizeSequenceName(const std::string& headerToken) {
    const size_t firstPipe = headerToken.find('|');
    if (firstPipe == std::string::npos) {
        return headerToken;
    }

    const size_t secondPipe = headerToken.find('|', firstPipe + 1);
    if (secondPipe == std::string::npos) {
        return headerToken;
    }

    const size_t thirdPipe = headerToken.find('|', secondPipe + 1);
    if (thirdPipe != std::string::npos) {
        return headerToken;
    }

    const std::string prefix = headerToken.substr(0, firstPipe);
    if (!isUniProtPrefix(prefix) || secondPipe == firstPipe + 1) {
        return headerToken;
    }

    return headerToken.substr(firstPipe + 1, secondPipe - firstPipe - 1);
}

} // namespace

// FastaReader implementation
FastaReader::FastaReader(const std::string& filename) {
    file.open(filename);
    if (!file) {
        throw IoException("Cannot open file: " + filename);
    }
}

FastaReader::~FastaReader() {
    if (file.is_open()) {
        file.close();
    }
}

std::vector<Sequence> FastaReader::readAll() {
    std::vector<Sequence> sequences;
    auto seq = readNext();
    while (seq) {
        sequences.push_back(*seq);
        seq = readNext();
    }
    return sequences;
}

std::unique_ptr<Sequence> FastaReader::readNext() {
    if (!file.is_open() || file.eof()) {
        return nullptr;
    }

    // Find next '>' character
    while (std::getline(file, currentLine)) {
        if (!currentLine.empty() && currentLine[0] == '>') {
            break;
        }
    }

    if (file.eof() && currentLine.empty()) {
        return nullptr;
    }

    // Parse the header line
    std::string name = readSequenceName();
    std::string sequence = readSequenceData();

    if (sequence.empty()) {
        throw IoException("Empty sequence for: " + name);
    }

    return std::make_unique<Sequence>(name, sequence);
}

std::string FastaReader::readSequenceName() {
    if (currentLine.empty() || currentLine[0] != '>') {
        throw IoException("Invalid FASTA format: header line must start with '>'");
    }

    std::string header = currentLine.substr(1);  // Skip '>'
    std::string name = normalizeSequenceName(extractHeaderToken(header));
    
    if (name.length() > MAX_SEQUENCE_NAME) {
        name = name.substr(0, MAX_SEQUENCE_NAME);
    }
    
    return name;
}

std::string FastaReader::readSequenceData() {
    std::string sequence;
    sequence.reserve(1024);  // Reserve some space

    // O(1) lookup table replacing the O(23) std::string::find per character
    static const auto makeAllowed = []() {
        std::array<bool, 256> t{};
        for (char c : {'A','R','N','D','C','Q','E','G','H','I','L','K','M','F','P','S','T','W','Y','V','B','Z','X'})
            t[static_cast<unsigned char>(c)] = true;
        return t;
    };
    static const std::array<bool, 256> allowed = makeAllowed();

    while (std::getline(file, currentLine)) {
        if (currentLine.empty()) continue;
        if (currentLine[0] == '>') {
            file.seekg(-static_cast<long>(currentLine.length() + 1), std::ios::cur);
            break;
        }

        // Process each character: convert to uppercase and check if it's allowed
        for (char c : currentLine) {
            char upperC = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            if (allowed[static_cast<unsigned char>(upperC)]) {
                sequence += upperC;
            }
        }
    }

    return sequence;
}

void FastaReader::reset() {
    file.clear();
    file.seekg(0);
}

} // namespace io
