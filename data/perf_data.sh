#!/usr/bin/env bash

echo
echo
echo
echo New run
echo `date` >> experiment.log
echo

../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-50.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-100.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-500.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-1000.mseq | tee -a experiment.log

