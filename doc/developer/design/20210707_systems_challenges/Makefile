CSV=$(wildcard *.csv)
SVGS=$(patsubst %.csv,%.svg, $(CSV))

%.svg: %.csv plot_idle_memory.gp
	gnuplot -c plot_idle_memory.gp $< > $@

all: $(SVGS)
