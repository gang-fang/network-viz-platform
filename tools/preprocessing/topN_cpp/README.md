# topN

topN identifies the top N most similar proteins to a query protein from a complete genome (ideally one with a high BUSCO score), based on full-length alignment. The code references to the EMBOSS [stretcher](https://emboss.sourceforge.net/apps/release/6.6/emboss/apps/stretcher.html) and [opscan](http://www.lcqb.upmc.fr/CHROnicle/SynChro.html) source codes.

topN is part of the *Protein Degree Centrality (ProtDC)* package, which provides tools and data for a function-oriented protein similarity metric called the Signal Jaccard Index (SJI). Other tools in the package include SoN (Signal over Noise), which uses spectral clustering to separate signals from noise, and SJINet, which builds a network using the SJI metric. For more details, please see this [reference](https://bmcbioinformatics.biomedcentral.com/articles/10.1186/s12859-024-06023-x). 

## Build

From the repository root:

```bash
make -C tools/preprocessing/topN_cpp
```

This compiles the C++ source under `src/` and installs the executable to:

```text
tools/bin/topn
```

Pipeline scripts use `tools/bin/topn` by default. Override it with the `TOPN` environment variable when needed:

```bash
TOPN=/path/to/topn bash tools/preprocessing/pipelines/build_your_net_scripts/02_run_topn_all_vs_all.sh
```
