# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/zhengy/dev/RTS/ips4o

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/zhengy/dev/RTS/ips4o/build

# Include any dependencies generated for this target.
include CMakeFiles/sort_intro.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/sort_intro.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/sort_intro.dir/flags.make

CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o: CMakeFiles/sort_intro.dir/flags.make
CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o: ../src/sort_intro.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zhengy/dev/RTS/ips4o/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o -c /home/zhengy/dev/RTS/ips4o/src/sort_intro.cpp

CMakeFiles/sort_intro.dir/src/sort_intro.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sort_intro.dir/src/sort_intro.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zhengy/dev/RTS/ips4o/src/sort_intro.cpp > CMakeFiles/sort_intro.dir/src/sort_intro.cpp.i

CMakeFiles/sort_intro.dir/src/sort_intro.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sort_intro.dir/src/sort_intro.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zhengy/dev/RTS/ips4o/src/sort_intro.cpp -o CMakeFiles/sort_intro.dir/src/sort_intro.cpp.s

CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o: CMakeFiles/sort_intro.dir/flags.make
CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o: ../src/introSort_kp_kv.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zhengy/dev/RTS/ips4o/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o -c /home/zhengy/dev/RTS/ips4o/src/introSort_kp_kv.cpp

CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zhengy/dev/RTS/ips4o/src/introSort_kp_kv.cpp > CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.i

CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zhengy/dev/RTS/ips4o/src/introSort_kp_kv.cpp -o CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.s

CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o: CMakeFiles/sort_intro.dir/flags.make
CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o: /home/zhengy/dev/RTS/utils_timer.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zhengy/dev/RTS/ips4o/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o -c /home/zhengy/dev/RTS/utils_timer.cpp

CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zhengy/dev/RTS/utils_timer.cpp > CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.i

CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zhengy/dev/RTS/utils_timer.cpp -o CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.s

# Object files for target sort_intro
sort_intro_OBJECTS = \
"CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o" \
"CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o" \
"CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o"

# External object files for target sort_intro
sort_intro_EXTERNAL_OBJECTS =

sort_intro: CMakeFiles/sort_intro.dir/src/sort_intro.cpp.o
sort_intro: CMakeFiles/sort_intro.dir/src/introSort_kp_kv.cpp.o
sort_intro: CMakeFiles/sort_intro.dir/home/zhengy/dev/RTS/utils_timer.cpp.o
sort_intro: CMakeFiles/sort_intro.dir/build.make
sort_intro: CMakeFiles/sort_intro.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/zhengy/dev/RTS/ips4o/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Linking CXX executable sort_intro"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/sort_intro.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/sort_intro.dir/build: sort_intro

.PHONY : CMakeFiles/sort_intro.dir/build

CMakeFiles/sort_intro.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/sort_intro.dir/cmake_clean.cmake
.PHONY : CMakeFiles/sort_intro.dir/clean

CMakeFiles/sort_intro.dir/depend:
	cd /home/zhengy/dev/RTS/ips4o/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zhengy/dev/RTS/ips4o /home/zhengy/dev/RTS/ips4o /home/zhengy/dev/RTS/ips4o/build /home/zhengy/dev/RTS/ips4o/build /home/zhengy/dev/RTS/ips4o/build/CMakeFiles/sort_intro.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/sort_intro.dir/depend
