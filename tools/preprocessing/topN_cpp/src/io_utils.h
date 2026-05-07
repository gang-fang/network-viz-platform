#ifndef IO_UTILS_H
#define IO_UTILS_H

#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <stdexcept>
#include "config.h"

namespace io {

/**
 * @brief Exception class for IO operations
 */
class IoException : public std::runtime_error {
public:
    explicit IoException(const std::string& message) 
        : std::runtime_error(message) {}
};

/**
 * @brief Class to handle FASTA format reading
 */
class FastaReader {
public:
    explicit FastaReader(const std::string& filename);
    ~FastaReader();

    // Read all sequences from the file
    std::vector<Sequence> readAll();
    
    // Read next sequence (returns nullptr if end of file)
    std::unique_ptr<Sequence> readNext();
    
    // Reset to beginning of file
    void reset();

private:
    std::ifstream file;
    std::string currentLine;
    
    // Helper functions
    std::string readSequenceName();
    std::string readSequenceData();
};

} // namespace io

#endif // IO_UTILS_H 
