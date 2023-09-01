#include <iostream>
#include "timer.h"

void Timer::start()
{
    start_ = high_resolution_clock::now();
}

void Timer::end()
{
    end_ = high_resolution_clock::now();
}

double Timer::get_time()
{
    time_span = std::chrono::duration_cast<duration<double>>(end_ - start_);
    return time_span.count();
}

void Timer::start(string name)
{
    auto it = start_map_.find(name);
    if (it != start_map_.end())
        it->second = high_resolution_clock::now();
    else
    {
        start_map_[name] = high_resolution_clock::now();
    }
}

void Timer::end(string name)
{
    auto it = end_map_.find(name);
    if (it != end_map_.end())
        it->second = high_resolution_clock::now();
    else
    {
        end_map_[name] = high_resolution_clock::now();
    }
    double period = get_time(name);
    if (period != -1)
        time_counter[name] += period;
    else
        std::cerr << "Timer::add_up: period is minus" << std::endl;
}

void Timer::drop_counter(string name)
{
    auto it = time_counter.find(name);
    if (it != time_counter.end())
        time_counter.erase(it);
}

void Timer::add_up(string name, double time)
{
    time_counter[name] += time;
}

double Timer::get_time(string name)
{
    auto it_start = start_map_.find(name);
    auto it_end = end_map_.find(name);

    if (it_start == start_map_.end() || it_end == end_map_.end())
        return -1;
    time_span = std::chrono::duration_cast<duration<double>>(it_end->second - it_start->second);
    return time_span.count();
}

double Timer::get_overall_time(string name)
{
    auto it = time_counter.find(name);

    if (it != time_counter.end())
        return it->second;
    else
        return -1;
}
