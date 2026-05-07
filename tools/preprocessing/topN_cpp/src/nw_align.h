#ifndef NW_ALIGN_H
#define NW_ALIGN_H

#include "config.h"  // for Sequence and ProgramConfig structs

/**
 * @brief Computes the ranking similarity percentage with the distance-only DP used in the hot path.
 *
 * This does not build a traceback. It preprocesses both sequences and applies
 * the global distance formulation used for scoring candidate hits.
 *
 * @param querySeq  The query sequence.
 * @param targetSeq  The target sequence.
 * @param config    Configuration settings (e.g., gap open and extend penalties).
 * @param freeEndGaps Reserved for API compatibility; currently ignored.
 * @return The similarity percentage (0.0-100.0).
 */
double computeNWSimilarityPercentage(
    const Sequence &querySeq,
    const Sequence &targetSeq,
    const ProgramConfig& config);

#endif  // NW_ALIGN_H
