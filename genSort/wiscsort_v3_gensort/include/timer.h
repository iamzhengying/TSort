#ifndef TIMER_H
#define TIMER_H
#include <chrono>
#include <string>
#include <unordered_map>
using std::string;
using std::chrono::duration;
using std::chrono::high_resolution_clock;

class Timer
{
private:
    std::unordered_map<string, double> time_counter;
    high_resolution_clock::time_point start_;
    high_resolution_clock::time_point end_;
    std::unordered_map<string, high_resolution_clock::time_point> start_map_;
    std::unordered_map<string, high_resolution_clock::time_point> end_map_;
    duration<double> time_span;

public:
    void start();
    void start(string name);
    void end();
    void end(string name);
    double get_time();
    double get_time(string name);
    double get_time(string start, string end);
    double get_overall_time(string name);
    void add_up(string name, double time);
    void drop_counter(string name);
};

#endif // _TIMER_H