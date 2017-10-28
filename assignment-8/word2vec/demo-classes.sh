make
if [ ! -e text8 ]; then
  #wget http://mattmahoney.net/dc/text8.zip -O text8.gz
  #gzip -d text8.gz -f
  curl -o text8.zip http://mattmahoney.net/dc/text8.zip
  unzip text8.zip
fi
time ./word2vec -train text8 -output classes.txt -cbow 0 -size 200 -window 5 -negative 0 -hs 1 -sample 1e-3 -threads 12 -classes 500
sort classes.txt -k 2 -n > classes.sorted.txt
echo The word classes were saved to file classes.sorted.txt
