# log_merge
Proof of concept to merge multiple log files into one time ordered log file

## Setup
```zsh
conda env create -f environment.yaml
conda activate log-merge
pre-commit install
```

and
```shell
pip install -e ".[dev,test]"
```

## Tests
```shell
pytest
```
