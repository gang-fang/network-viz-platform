#include "fastp.h"
#include "candidate.h"
#include "matrix.h"
#include "speedup.h"
#include <vector>
#include <algorithm>
#include <cctype>
#include <string>
#include <cstdint>
#include <thread>
#include <atomic>

int computeBitShift(int alphabetSize) {
    int l = 0;
    while ((1 << l) < alphabetSize) l++;
    return l;
}

std::vector<int> indexSequence(const std::string& seq, const std::vector<int>& charToIdx) {
    std::vector<int> indexed(seq.size());
    for (size_t i = 0; i < seq.size(); i++) {
        indexed[i] = charToIdx[static_cast<unsigned char>(std::toupper(seq[i]))];
    }
    return indexed;
}

uint32_t computeHash(const std::vector<int>& seq, int pos, int kmerSize, int bitshift, uint32_t wmask) {
    uint32_t h = 0;
    for (int i = 0; i < kmerSize; i++) {
        h = (h << bitshift) | static_cast<uint32_t>(seq[pos + i]);
    }
    return h & wmask;
}

uint32_t updateHashValue(uint32_t old, int newSymbol, int bitshift, uint32_t wmask) {
    return ((old << bitshift) & wmask) | static_cast<uint32_t>(newSymbol);
}

// Direct-addressed hash table.
// For k=2 / 23 amino acids (bitshift=5): tsize=1024, fits entirely in L1 cache.
using DirectHash = std::vector<std::vector<int>>;

// Flat normalized similarity lookup:
// sim = (score - smin) / (smax - smin), with identical residues forced to smax.
static std::vector<float> buildScoreFlat(const std::vector<char>& aminoAcids, int bitshift) {
    uint32_t tsize = 1u << (2 * bitshift);
    std::vector<float> flat(tsize, 0.0f);

    int smin = 0;
    int smax = 0;
    bool init = false;
    for (char aa1 : aminoAcids) {
        for (char aa2 : aminoAcids) {
            int score = substitutionMatrix[aa1 - 'A'][aa2 - 'A'];
            if (!init) {
                smin = smax = score;
                init = true;
            } else {
                smin = std::min(smin, score);
                smax = std::max(smax, score);
            }
        }
    }

    const float denom = static_cast<float>(std::max(1, smax - smin));
    for (size_t i = 0; i < aminoAcids.size(); i++) {
        for (size_t j = 0; j < aminoAcids.size(); j++) {
            int score = (i == j)
                ? smax
                : substitutionMatrix[aminoAcids[i] - 'A'][aminoAcids[j] - 'A'];
            flat[(i << bitshift) | j] =
                static_cast<float>(score - smin) / denom;
        }
    }

    return flat;
}

struct DiagonalSelection {
    int bestDiag = 0;
    int bestVotes = 0;
};

static DiagonalSelection selectBestDiagonal(const std::vector<int>& diagVotes,
                                            int diagSize,
                                            int diagOffset,
                                            int diagInteg) {
    if (diagSize <= 0) return {};

    const int window = 2 * diagInteg + 1;
    if (window <= 1 || diagSize <= 1) {
        int maxVotes = diagVotes[0];
        int maxDiag = 0;
        int maxMiddle = diagVotes[0];
        int minDelta = std::abs(-diagOffset);

        for (int i = 1; i < diagSize; ++i) {
            if (diagVotes[i] < maxVotes) continue;

            const int delta = std::abs(i - diagOffset);
            const int middle = diagVotes[i];
            if (diagVotes[i] == maxVotes) {
                if (middle < maxMiddle) continue;
                if (middle == maxMiddle && delta > minDelta) continue;
            }

            maxVotes = diagVotes[i];
            maxDiag = i;
            maxMiddle = middle;
            minDelta = delta;
        }

        return {maxDiag - diagOffset, maxVotes};
    }

    if (diagSize <= 2 * diagInteg) {
        int bestIdx = 0;
        int bestVotes = 0;
        for (int i = 0; i < diagSize; ++i) {
            bestVotes += diagVotes[i];
            if (diagVotes[i] > diagVotes[bestIdx]) bestIdx = i;
        }
        return {bestIdx - diagOffset, bestVotes};
    }

    int sum = 0;
    for (int i = 0; i < window; ++i) sum += diagVotes[i];

    int maxVotes = sum;
    int maxDiag = diagInteg;
    int maxMiddle = diagVotes[diagInteg];
    int minDelta = std::abs(diagInteg - diagOffset);

    for (int i = diagInteg + 1; i < diagSize - diagInteg; ++i) {
        sum += diagVotes[i + diagInteg] - diagVotes[i - diagInteg - 1];
        if (sum < maxVotes) continue;

        const int delta = std::abs(i - diagOffset);
        const int middle = diagVotes[i];
        if (sum == maxVotes) {
            if (middle < maxMiddle) continue;
            if (middle == maxMiddle && delta > minDelta) continue;
        }

        maxVotes = sum;
        maxDiag = i;
        maxMiddle = middle;
        minDelta = delta;
    }

    return {maxDiag - diagOffset, maxVotes};
}

// -----------------------------------------------------------------------
// Aliscore: sum of BLOSUM scores along the aligned region of the best
// diagonal, normalized by min sequence length.
//
// Unlike hspScore (which has max(0, …) giving a loop-carried dependency
// that blocks SIMD), this is a plain reduction — the compiler vectorizes
// it automatically with SSE/AVX, giving ~4–8× faster inner loop.
//
// Using pre-indexed integer sequences + flat score table avoids char-'A'
// arithmetic and accesses a 4 KB array that stays in L1 cache.
// -----------------------------------------------------------------------
static float aliscore(const std::vector<int>& qIdx, const std::vector<int>& tIdx,
                      int offset, int bitshift, const std::vector<float>& scoreFlat) {
    int len1 = static_cast<int>(qIdx.size());
    int len2 = static_cast<int>(tIdx.size());
    int t_start = std::max(0, offset);
    int t_end   = std::min(len2, len1 + offset);
    int overlap  = t_end - t_start;
    if (overlap <= 0) return 0.0f;
    int q_start = t_start - offset;

    float score = 0.0f;
    for (int k = 0; k < overlap; k++)
        score += scoreFlat[(qIdx[q_start + k] << bitshift) | tIdx[t_start + k]];
    return score / static_cast<float>(std::min(len1, len2));
}

bool isLengthRatioAcceptable(const Sequence& query, const Sequence& target, float lengthRatio) {
    double ratio = (query.sequence.size() >= target.sequence.size()) ?
        static_cast<double>(query.sequence.size()) / target.sequence.size() :
        static_cast<double>(target.sequence.size()) / query.sequence.size();
    return ratio <= lengthRatio;
}

// -----------------------------------------------------------------------
// Hot path: k-mer prefilter.
//
// Two key design points:
//
// 1. Hash the QUERY, scan each TARGET through it.
//    The small query hash table (~24 KB for k=2) stays hot in L1 cache
//    while target sequences are streamed past with a rolling hash.
//
// 2. After finding the best diagonal, score it with Aliscore.
//    This is still linear in overlap, but with pre-indexed sequences and
//    a flat substitution table it remains cheap.
// -----------------------------------------------------------------------
std::vector<CandidateList> runFastpPrefilter(const std::vector<Sequence>& queries,
                                             const std::vector<Sequence>& db,
                                             const ProgramConfig& config,
                                             const std::vector<char>& aminoAcids) {
    std::vector<CandidateList> allCandidates(queries.size());

    std::vector<int> charToIdx(256, 0);
    for (size_t i = 0; i < aminoAcids.size(); i++)
        charToIdx[static_cast<unsigned char>(aminoAcids[i])] = i;

    int bitshift = computeBitShift(aminoAcids.size());
    uint32_t wmask = (1u << (config.kmerSize * bitshift)) - 1;
    uint32_t tsize = wmask + 1;   // 1024 for k=2 / 23 AAs
    int kmerSize = config.kmerSize;
    const int targetPrefilterCount = config.targetPrefilterCount();
    const int maxPrefilterCount = config.maxPrefilterCount();
    const auto scoreFlat = buildScoreFlat(aminoAcids, bitshift);

    // Pre-index all sequences once.
    std::vector<std::vector<int>> indexedQueries(queries.size());
    std::vector<std::vector<int>> indexedDb(db.size());
    for (size_t i = 0; i < queries.size(); i++)
        indexedQueries[i] = indexSequence(queries[i].sequence, charToIdx);
    for (size_t i = 0; i < db.size(); i++)
        indexedDb[i] = indexSequence(db[i].sequence, charToIdx);

    unsigned int configuredThreads = config.numThreads == 0
        ? std::thread::hardware_concurrency()
        : static_cast<unsigned int>(config.numThreads);
    const unsigned int numThreads = std::max(1u,
        std::min<unsigned int>(configuredThreads,
                               static_cast<unsigned int>(queries.size())));
    std::atomic<size_t> nextQuery{0};

    auto worker = [&]() {
        // Per-thread query hash table: tsize slots, reused across queries.
        // For k=2 this is 1024 vectors (~24 KB metadata) — always in L1.
        DirectHash qHash(tsize);
        // Track filled slots to avoid clearing the entire 1024-entry table.
        std::vector<uint32_t> usedSlots;

        // Per-thread diagonal vote array.
        const int defaultDiagSize = 3001;
        std::vector<int> diagVotes(defaultDiagSize, 0);

        while (true) {
            size_t q = nextQuery.fetch_add(1);
            if (q >= queries.size()) break;

            const auto& qIdx = indexedQueries[q];
            int qlen = static_cast<int>(qIdx.size());

            // ---- Build query hash table ----
            // Clear only slots touched by the previous query.
            for (uint32_t slot : usedSlots) qHash[slot].clear();
            usedSlots.clear();

            if (qlen >= kmerSize) {
                uint32_t h = computeHash(qIdx, 0, kmerSize, bitshift, wmask);
                if (qHash[h].empty()) usedSlots.push_back(h);
                qHash[h].push_back(0);
                for (int i = 1; i + kmerSize <= qlen; i++) {
                    h = updateHashValue(h, qIdx[i + kmerSize - 1], bitshift, wmask);
                    if (qHash[h].empty()) usedSlots.push_back(h);
                    qHash[h].push_back(i);
                }
            }

            // ---- Scan each target (diagonal accumulation + scoring) ----
            CandidateList candidates;
            for (size_t t = 0; t < db.size(); ++t) {
                const auto& tIdx = indexedDb[t];
                int tlen = static_cast<int>(tIdx.size());

                if (tlen < kmerSize) continue;
                if (!isLengthRatioAcceptable(queries[q], db[t], config.lengthRatio)) continue;

                // Diagonal array sizing
                int diagOffset = 1500;
                int diagSize = defaultDiagSize;
                if (qlen > 1500 || tlen > 1500) {
                    diagOffset = qlen;
                    diagSize = qlen + tlen;
                    if (diagSize > static_cast<int>(diagVotes.size()))
                        diagVotes.resize(diagSize, 0);
                }

                std::fill(diagVotes.begin(), diagVotes.begin() + diagSize, 0);

                // Rolling hash over TARGET, look up in QUERY hash table.
                // qHash (~24 KB) stays in L1; target sequences are streamed in order.
                bool seedFound = false;
                uint32_t h = computeHash(tIdx, 0, kmerSize, bitshift, wmask);
                {
                    const auto& qPos = qHash[h];
                    if (!qPos.empty()) {
                        seedFound = true;
                        for (int qp : qPos) {
                            int diag = -qp + diagOffset;  // tpos=0, diag = 0 - qp + off
                            if (diag >= 0 && diag < diagSize) diagVotes[diag]++;
                        }
                    }
                }
                for (int j = 1; j + kmerSize <= tlen; j++) {
                    h = updateHashValue(h, tIdx[j + kmerSize - 1], bitshift, wmask);
                    const auto& qPos = qHash[h];
                    if (!qPos.empty()) {
                        seedFound = true;
                        for (int qp : qPos) {
                            int diag = j - qp + diagOffset;
                            if (diag >= 0 && diag < diagSize) diagVotes[diag]++;
                        }
                    }
                }

                if (!seedFound) continue;

                DiagonalSelection best = selectBestDiagonal(diagVotes, diagSize, diagOffset, 0);
                float score = aliscore(qIdx, tIdx, best.bestDiag, bitshift, scoreFlat);

                if (score < config.lowerThreshold) continue;

                Candidate cand;
                cand.targetName = db[t].name;
                cand.fastpScore = score;
                candidates.push_back(std::move(cand));
            }

            if (targetPrefilterCount > 0 &&
                candidates.size() > static_cast<size_t>(targetPrefilterCount)) {
                auto nth = candidates.begin() + targetPrefilterCount;
                std::nth_element(candidates.begin(), nth, candidates.end(),
                                 [](const Candidate& a, const Candidate& b) {
                                     return a.fastpScore > b.fastpScore;
                                 });
                const double cutoffScore = nth->fastpScore;
                auto keepEnd = std::partition(candidates.begin(), candidates.end(),
                                              [cutoffScore](const Candidate& cand) {
                                                  return cand.fastpScore >= cutoffScore;
                                              });
                candidates.resize(static_cast<size_t>(keepEnd - candidates.begin()));
                std::sort(candidates.begin(), candidates.end(),
                          [](const Candidate& a, const Candidate& b) {
                              if (a.fastpScore != b.fastpScore)
                                  return a.fastpScore > b.fastpScore;
                              return a.targetName < b.targetName;
                          });
                if (maxPrefilterCount > 0 &&
                    candidates.size() > static_cast<size_t>(maxPrefilterCount)) {
                    candidates.resize(static_cast<size_t>(maxPrefilterCount));
                }
            } else {
                std::sort(candidates.begin(), candidates.end(),
                          [](const Candidate& a, const Candidate& b) {
                              if (a.fastpScore != b.fastpScore)
                                  return a.fastpScore > b.fastpScore;
                              return a.targetName < b.targetName;
                          });
            }
            allCandidates[q] = std::move(candidates);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (unsigned int i = 0; i < numThreads; ++i)
        threads.emplace_back(worker);
    for (auto& th : threads) th.join();

    return allCandidates;
}
