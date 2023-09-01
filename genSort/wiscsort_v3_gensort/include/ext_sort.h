#include "data_manager.h"

using std::string;
using std::vector;

class ExtSort
{
public:
    ExtSort(std::string mount_path);
    void Sort(vector<string> &files);

private:
    string folder_path_;
    vector<string> RunGeneration(vector<string> files);
    size_t InMemorySort(size_t start, DataManager &m, string output_file);
    void MergeRuns(vector<string> runs, string input_file);
};