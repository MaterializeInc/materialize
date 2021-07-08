set terminal svg

set datafile separator ','
set logscale x 2
set logscale y 2
set format y "%.2f"

set ylabel "Rss (GiB)"
set xlabel "Threads"

plot ARG1 using 1:($2/(1024*1024)) with linespoints title "Rss (GiB)"
