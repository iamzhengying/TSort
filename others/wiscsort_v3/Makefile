test: test.cc data_manager.cc ext_sort.cc timer.cc
	g++ -std=c++17 -I./include/ -I../../include/ips4o/include/ -Wall test.cc data_manager.cc ext_sort.cc timer.cc -g3 -O0 -pthread -ltbb -latomic -fnon-call-exceptions -o test
leak: test.cc data_manager.cc ext_sort.cc timer.cc
	g++ -std=c++17 -I../wiscsort/include -Wall test.cc data_manager.cc ext_sort.cc timer.cc -g3 -O0 -fsanitize=address -static-libasan -o leak
release: test.cc data_manager.cc ext_sort.cc timer.cc
	g++ -std=c++17 -I./include/ -I../../include/ips4o/include/ -Wall test.cc data_manager.cc ext_sort.cc timer.cc -O3 -pthread -ltbb -latomic -o release
clean:
	rm -rf test leak release
staged-pmdk: test.cc data_manager.cc ext_sort.cc timer.cc
	g++ -std=c++17 -I../wiscsort/include -Wall test.cc data_manager.cc ext_sort.cc timer.cc -O3 -pthread -ltbb -latomic -march=skylake -Dpmdk -Dbandwidth -lpmem -o release
