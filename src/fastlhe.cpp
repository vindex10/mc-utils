#include <vector>
#include <iostream>
#include <string.h>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;
const int PARTICLE_NUM_VALUES = 14;

char* skip_seps(char* from, const char* sep) {
    char* end = from;
    for (;*end == *sep;++end); // skip spaces before first char of the next token
    return end;
}

char* skip_token(char* from, const char* sep) {
    char* end = from;
    end = skip_seps(end, sep);
    for (;*end != *sep;++end); // skip current token
    end = skip_seps(end, sep);
    return end;
}


py::array parse_batch(const int num_particles, char *pre_batch, char *input_batch) {
    char* cursor = pre_batch;
    char* token_end;
    int cur_num_particles;
    int num_events = 0;
    double weight;

    std::vector<double>* res = new std::vector<double>();
    cursor = strstr(pre_batch, "<event");
    while (cursor && *cursor != '\0') {
        cursor = strstr(cursor, "\n") + 1;  // local cursor at meta
        cursor = skip_seps(cursor, " ");

        cur_num_particles = strtol(cursor, &cursor, 10);
        //std::cout << "Num particles: " << cur_num_particles;

        if (num_particles != cur_num_particles) {
            //std::cout << std::endl << "Num particles mismatch (" << cur_num_particles << "!= " << num_particles << "). Skipping" << std::endl;
            cursor = strstr(cursor, "<event");
            continue;
        }

        cursor = skip_token(cursor, " ");  // skip second col
        weight = strtod(cursor, &token_end);
        //std::cout << " Weight: " << weight << std::endl;

        cursor = strstr(token_end, "\n") + 1;  // line with first particle

        res->reserve(num_particles*PARTICLE_NUM_VALUES);
        for (int p = 0; p < num_particles; ++p) {
            for (int i = 0; i < PARTICLE_NUM_VALUES-1; i++) {
                cursor = skip_seps(cursor, " ");  // skip second col
                double quantity = strtod(cursor, &cursor);
                res->push_back(quantity);
            }
            res->push_back(weight);
            cursor = strstr(cursor, "\n") + 1;
        }
        num_events += 1;
        //std::cout << num_events << std::endl;
    }

    auto capsule = py::capsule(res, [] (void *v) {delete reinterpret_cast<std::vector<double>*>(v);});
    return py::array({num_events, num_particles, PARTICLE_NUM_VALUES},
                     res->data(), capsule);
}

PYBIND11_MODULE(fastlhe, m) {
    m.doc() = "fast lhe format utils"; // optional module docstring

    m.def("parse_batch", &parse_batch, "Parse extract from lhe into numpy array");
}
