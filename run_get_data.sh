#!/bin/zsh
eval "$(conda shell.zsh hook)"
conda activate distributed
python3 get_data.py $1 $2 $3 $4 $5 $6
#echo $1, $2, $3, $4, $5, $6