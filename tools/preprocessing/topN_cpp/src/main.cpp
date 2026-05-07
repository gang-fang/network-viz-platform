#include "config.h"
#include "fastp.h"
#include "io_utils.h"
#include "matrix.h"
#include "nw_align.h"
#include "speedup.h"
#include <algorithm>
#include <atomic>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

std::string sanitizeProteinId(const std::string &proteinId) {
  std::string sanitized;
  sanitized.reserve(proteinId.size());
  for (char ch : proteinId) {
    if (ch != '"' && ch != '\'') {
      sanitized.push_back(ch);
    }
  }
  return sanitized;
}

std::string escapeJsonString(const std::string &value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (unsigned char ch : value) {
    switch (ch) {
    case '\\':
      escaped += "\\\\";
      break;
    case '\b':
      escaped += "\\b";
      break;
    case '\f':
      escaped += "\\f";
      break;
    case '\n':
      escaped += "\\n";
      break;
    case '\r':
      escaped += "\\r";
      break;
    case '\t':
      escaped += "\\t";
      break;
    default:
      if (ch < 0x20) {
        static const char hex[] = "0123456789abcdef";
        escaped += "\\u00";
        escaped.push_back(hex[(ch >> 4) & 0x0F]);
        escaped.push_back(hex[ch & 0x0F]);
      } else {
        escaped.push_back(static_cast<char>(ch));
      }
      break;
    }
  }
  return escaped;
}

// Function to print usage information
void printUsage(const char *programName) {
  std::cerr << "Usage: " << programName << " [options]" << std::endl;
  std::cerr << "Options:" << std::endl;
  std::cerr << "  -q, --query FILE      Query sequence file in FASTA format "
               "(required)"
            << std::endl;
  std::cerr << "  -t, --target FILE     Target sequence file in FASTA format "
               "(required)"
            << std::endl;
  std::cerr << "  -o, --output FILE     Output file (default: stdout)"
            << std::endl;
  std::cerr << "  -m, --outfmt FORMAT   Output format (j=json, m=markdown, "
               "c=csv). Default: c"
            << std::endl;
  std::cerr
      << "  -M, --matrix FILE     Substitution matrix file. Default: BLOSUM62"
      << std::endl;
  std::cerr << "  -k, --kmer INT        K-mer size for FASTP (default: "
            << DEFAULT_KMER_SIZE << ")" << std::endl;
  std::cerr << "  -n, --topn INT        Number of top hits (default: "
            << DEFAULT_TOP_N << ")" << std::endl;
  std::cerr << "  -s, --tops FLOAT      Minimum hit score (similarity %) to "
               "keep (default: "
            << DEFAULT_MIN_HIT_SCORE << ")" << std::endl;
  std::cerr << "  -l, --threshold FLOAT Lower threshold for FASTP (default: "
            << DEFAULT_LOWER_THRESHOLD << ")" << std::endl;
  std::cerr << "  -r, --ratio FLOAT     Maximum allowed length ratio (default: "
            << DEFAULT_LENGTH_RATIO << ")" << std::endl;
  std::cerr << "  -g, --gapopen INT     Gap open penalty (default: 120)"
            << std::endl;
  std::cerr << "  -e, --gapextend INT   Gap extend penalty (default: 80)"
            << std::endl;
  std::cerr << "  -p, --threads INT     Number of threads to use (default: 1; "
               "0 = auto)"
            << std::endl;
  std::cerr << "  -v, --verbose         Enable verbose output" << std::endl;
}

// Function to parse command-line arguments into ProgramConfig
ProgramConfig parseArguments(int argc, char *argv[]) {
  ProgramConfig config;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];

    if (arg == "-q" || arg == "--query") {
      if (i + 1 < argc)
        config.queryFile = argv[++i];
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-t" || arg == "--target") {
      if (i + 1 < argc)
        config.targetFile = argv[++i];
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-o" || arg == "--output") {
      if (i + 1 < argc)
        config.outputFile = argv[++i];
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-m" || arg == "--outfmt") {
      if (i + 1 < argc) {
        std::string value = argv[++i];
        if (value == "j" || value == "m" || value == "c")
          config.outputFormat = value;
        else {
          std::cerr << "Invalid output format: " << value << std::endl;
          printUsage(argv[0]);
          exit(1);
        }
      } else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-M" || arg == "--matrix") {
      if (i + 1 < argc)
        config.scoringMatrixFile = argv[++i];
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-k" || arg == "--kmer") {
      if (i + 1 < argc)
        config.kmerSize = std::stoi(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-n" || arg == "--topn") {
      if (i + 1 < argc)
        config.topN = std::stoi(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-s" || arg == "--tops") {
      if (i + 1 < argc)
        config.minHitScore = std::stof(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-l" || arg == "--threshold") {
      if (i + 1 < argc)
        config.lowerThreshold = std::stof(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-r" || arg == "--ratio") {
      if (i + 1 < argc)
        config.lengthRatio = std::stof(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-g" || arg == "--gapopen") {
      if (i + 1 < argc)
        config.gapOpenPenalty = std::stoi(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-e" || arg == "--gapextend") {
      if (i + 1 < argc)
        config.gapExtendPenalty = std::stoi(argv[++i]);
      else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else if (arg == "-v" || arg == "--verbose") {
      config.verbose = true;
    } else if (arg == "-p" || arg == "--threads") {
      if (i + 1 < argc) {
        config.numThreads = std::stoi(argv[++i]);
        if (config.numThreads < 0) {
          std::cerr << "Invalid thread count: " << config.numThreads
                    << std::endl;
          exit(1);
        }
      } else {
        std::cerr << "Missing value for " << arg << std::endl;
        printUsage(argv[0]);
        exit(1);
      }
    } else {
      std::cerr << "Unknown option: " << arg << std::endl;
      printUsage(argv[0]);
      exit(1);
    }
  }

  if (config.queryFile.empty() || config.targetFile.empty()) {
    std::cerr << "Error: Both query (-q) and target (-t) files are required"
              << std::endl;
    printUsage(argv[0]);
    exit(1);
  }
  return config;
}

int main(int argc, char *argv[]) {
  ProgramConfig config = parseArguments(argc, argv);

  io::FastaReader queryReader(config.queryFile);
  std::vector<Sequence> querySeqs;
  try {
    querySeqs = queryReader.readAll();
  } catch (const std::exception &e) {
    std::cerr << "Error reading query file: " << e.what() << std::endl;
    return 1;
  }
  if (querySeqs.empty()) {
    std::cerr << "No sequences found in query file." << std::endl;
    return 1;
  }
  if (config.verbose) {
    std::cout << "# Using all " << querySeqs.size() << " query sequences from "
              << config.queryFile << "." << std::endl;
  }

  io::FastaReader targetReader(config.targetFile);
  std::vector<Sequence> targetSeqs;
  try {
    targetSeqs = targetReader.readAll();
  } catch (const std::exception &e) {
    std::cerr << "Error reading target file: " << e.what() << std::endl;
    return 1;
  }
  if (targetSeqs.empty()) {
    std::cerr << "No sequences found in target file." << std::endl;
    return 1;
  }
  if (config.verbose) {
    std::cout << "# Loaded " << targetSeqs.size() << " target sequences from "
              << config.targetFile << "." << std::endl;
  }

  std::vector<std::vector<int>> matrix;
  std::vector<char> aminoAcids;
  if (!config.scoringMatrixFile.empty()) {
    if (config.verbose)
      std::cout << "# Using substitution matrix from file: "
                << config.scoringMatrixFile << std::endl;
    try {
      if (!loadSubstitutionMatrix(config.scoringMatrixFile, matrix,
                                  aminoAcids)) {
        std::cerr << "Failed to load substitution matrix from "
                  << config.scoringMatrixFile << std::endl;
        return 1;
      }
    } catch (const std::exception &e) {
      std::cerr << "Error loading matrix file: " << e.what() << std::endl;
      return 1;
    }
  } else {
    if (config.verbose)
      std::cout << "# Using default BLOSUM62 matrix" << std::endl;
    try {
      std::istringstream matrixStream(getDefaultBLOSUM62Matrix());
      if (!parseSubstitutionMatrix(matrixStream, matrix, aminoAcids)) {
        std::cerr << "Failed to parse default BLOSUM62 matrix" << std::endl;
        return 1;
      }
    } catch (const std::exception &e) {
      std::cerr << "Error loading default matrix: " << e.what() << std::endl;
      return 1;
    }
  }

  if (config.verbose)
    std::cout << std::endl << std::endl;

  precomputeSubstitutionScores(matrix, aminoAcids);

  std::vector<CandidateList> candidateLists;
  try {
    candidateLists =
        runFastpPrefilter(querySeqs, targetSeqs, config, aminoAcids);
  } catch (const std::exception &e) {
    std::cerr << "Error during FASTP prefiltering: " << e.what() << std::endl;
    return 1;
  }

  bool anyCandidates = false;
  for (size_t i = 0; i < querySeqs.size(); ++i) {
    if (candidateLists[i].empty()) {
      std::cerr << "No candidate matches found from FASTP prefilter for query: "
                << querySeqs[i].name << " against target: " << config.targetFile
                << std::endl;
    } else {
      anyCandidates = true;
    }
  }

  if (!anyCandidates) {
    return 0;
  }

  struct NWResult {
    std::string targetName;
    double nwSimilarity;
    size_t targetLength;
    double fastpScore;
    std::string queryName;
    size_t queryLength;
  };
  std::vector<NWResult> nwResults;

  std::unordered_map<std::string, size_t> targetIndex;
  targetIndex.reserve(targetSeqs.size());
  for (size_t i = 0; i < targetSeqs.size(); ++i) {
    targetIndex[targetSeqs[i].name] = i;
  }

  std::mutex resultsMutex;
  unsigned int configuredThreads =
      config.numThreads == 0 ? std::thread::hardware_concurrency()
                             : static_cast<unsigned int>(config.numThreads);
  const unsigned int numThreads = std::max(
      1u,
      std::min<unsigned int>(configuredThreads,
                             static_cast<unsigned int>(candidateLists.size())));
  std::atomic<size_t> nextQuery{0};
  auto nwWorker = [&]() {
    std::vector<NWResult> localResults;
    while (true) {
      size_t q = nextQuery.fetch_add(1);
      if (q >= candidateLists.size())
        break;
      const Sequence &currentQuery = querySeqs[q];
      const CandidateList &candidates = candidateLists[q];

      for (const auto &cand : candidates) {
        auto it = targetIndex.find(cand.targetName);
        if (it != targetIndex.end()) {
          const Sequence &tgt = targetSeqs[it->second];
          double similarity =
              computeNWSimilarityPercentage(currentQuery, tgt, config);
          if (similarity >= config.minHitScore) {
            localResults.push_back({cand.targetName, similarity,
                                    tgt.sequence.size(), cand.fastpScore,
                                    currentQuery.name,
                                    currentQuery.sequence.size()});
          }
        }
      }
    }
    if (!localResults.empty()) {
      std::lock_guard<std::mutex> lock(resultsMutex);
      nwResults.insert(nwResults.end(), localResults.begin(),
                       localResults.end());
    }
  };

  std::vector<std::thread> nwThreads;
  nwThreads.reserve(numThreads);
  for (unsigned int i = 0; i < numThreads; ++i) {
    nwThreads.emplace_back(nwWorker);
  }
  for (auto &th : nwThreads)
    th.join();

  std::sort(nwResults.begin(), nwResults.end(),
            [](const NWResult &a, const NWResult &b) {
              if (a.queryName != b.queryName)
                return a.queryName < b.queryName;
              return a.nwSimilarity > b.nwSimilarity;
            });

  std::map<std::string, std::vector<NWResult>> resultsByQuery;
  for (const auto &res : nwResults) {
    resultsByQuery[res.queryName].push_back(res);
  }
  for (auto &[queryName, results] : resultsByQuery) {
    std::sort(results.begin(), results.end(),
              [](const NWResult &a, const NWResult &b) {
                if (a.nwSimilarity != b.nwSimilarity)
                  return a.nwSimilarity > b.nwSimilarity;
                return a.targetName < b.targetName;
              });
    if (config.topN > 0 &&
        results.size() > static_cast<size_t>(config.topN)) {
      results.resize(static_cast<size_t>(config.topN));
    }
  }

  std::ostream *out = &std::cout;
  std::ofstream fileOut;
  if (!config.outputFile.empty()) {
    fileOut.open(config.outputFile);
    if (!fileOut.is_open()) {
      std::cerr << "Failed to open output file: " << config.outputFile
                << std::endl;
      return 1;
    }
    out = &fileOut;
  }

  if (config.outputFormat == "m") {
    for (const auto &[queryName, results] : resultsByQuery) {
      size_t queryLength = results[0].queryLength;
      const std::string sanitizedQueryName = sanitizeProteinId(queryName);
      *out << "## Query: " << sanitizedQueryName << " (Length: " << queryLength
           << ")" << std::endl;
      *out << "| ID | Score | Length |" << std::endl;
      *out << "|----|-------|--------|" << std::endl;
      for (const auto &res : results) {
        const std::string sanitizedTargetName = sanitizeProteinId(res.targetName);
        *out << "| " << sanitizedTargetName << " | " << std::fixed
             << std::setprecision(2) << res.nwSimilarity << " | "
             << res.targetLength << " |" << std::endl;
      }
      *out << std::endl;
    }
  } else if (config.outputFormat == "c") {
    *out << "\"Query ID\",\"Query Length\",\"Hit ID\",\"Hit Score\",\"Hit "
            "Length\""
         << std::endl;
    for (const auto &[queryName, results] : resultsByQuery) {
      size_t queryLength = results[0].queryLength;
      const std::string sanitizedQueryName = sanitizeProteinId(queryName);
      for (const auto &res : results) {
        const std::string sanitizedTargetName = sanitizeProteinId(res.targetName);
        *out << sanitizedQueryName << "," << queryLength << ","
             << sanitizedTargetName << "," << std::fixed
             << std::setprecision(2) << res.nwSimilarity << ","
             << res.targetLength << std::endl;
      }
    }
  } else { // JSON output
    *out << "[" << std::endl;
    bool firstQuery = true;
    for (const auto &[queryName, results] : resultsByQuery) {
      if (!firstQuery)
        *out << "," << std::endl;
      firstQuery = false;
      size_t queryLength = results[0].queryLength;
      const std::string sanitizedQueryName =
          escapeJsonString(sanitizeProteinId(queryName));
      *out << "  {" << std::endl;
      *out << "    \"query_id\": \"" << sanitizedQueryName << "\","
           << std::endl;
      *out << "    \"query_length\": " << queryLength << "," << std::endl;
      *out << "    \"hits\": [" << std::endl;
      bool firstHit = true;
      for (const auto &res : results) {
        if (!firstHit)
          *out << "," << std::endl;
        firstHit = false;
        const std::string sanitizedTargetName =
            escapeJsonString(sanitizeProteinId(res.targetName));
        *out << "      {" << std::endl;
        *out << "        \"hit\": \"" << sanitizedTargetName << "\","
             << std::endl;
        *out << "        \"hit_score\": " << std::fixed << std::setprecision(2)
             << res.nwSimilarity << "," << std::endl;
        *out << "        \"hit_length\": " << res.targetLength << std::endl;
        *out << "      }";
      }
      *out << std::endl << "    ]" << std::endl;
      *out << "  }";
    }
    *out << std::endl << "]" << std::endl;
  }

  if (fileOut.is_open())
    fileOut.close();
  return 0;
}
