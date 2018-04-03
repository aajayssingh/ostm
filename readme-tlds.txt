compile:
trans-compile> make -j8

execute:
trans-compile> ./src/trans [0/1/2] #threads testSize transsize keyRange inspercent del percent

0--> transactional list
1--> RSTM list (Norec)
2--> Boosting list

source code main file:
trans-dev/src/bench main.cc

