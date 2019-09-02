# FEVERlite
> a simplified version of the FEVER dataset for educational use
> based on the Fact Extraction and VERification dataset from Thorne et al. 2018

### Install environment 
```bash
conda create -n feverlite python=3.6
source activate feverlite
pip install -r requirements.txt
```

### Create dataset

```bash
python -m feverlite.dataset.construction.generate \
    --train data/train.jsonl \
    --validation data/validation.jsonl \
    --test data/test.jsonl 
```