Pathogenic Variant Filtering
===========================

This module filters variants for pathogenicity based on a set of criteria.

# Installation
## Installation through Oakvar

From Oakvar store:
```bash
ov module install just_pathogenic
```
From a direct link on GitHub:
```bash
ov module install just_pathogenic --url https://github.com/dna-seq/just_pathogenic/tree/main
```


## Debugging notes

If you want to debug the module, then use the conda/micromamba environment where oakvar is installed (for example quick-dna).

```bash
micromamba activate quick-dna
```
Then you can run the module with:
```bash
ov run -p just_pathogenic
```
