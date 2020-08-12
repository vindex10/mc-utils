#include <vector>
#include <iostream>
#include <string.h>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;
const int PARTICLE_NUM_VALUES = 14;


const char* skip_seps(const char* from, const char* sep) {
    const char* end = from;
    for (;*end == *sep;++end); // skip spaces before first char of the next token
    return end;
}

const char* skip_token(const char* from, const char* sep) {
    const char* end = from;
    end = skip_seps(end, sep);
    for (;*end != *sep;++end); // skip current token
    end = skip_seps(end, sep);
    return end;
}

const char *nextline(const char* cursor) {
    cursor = std::strstr(cursor, "\n") + 1;
    return cursor;
}

int process_event(const int num_particles, const char* cursor, std::vector<double>* res) {
    char* endpos;

    cursor = nextline(cursor);  // local cursor at meta
    cursor = skip_seps(cursor, " ");

    int cur_num_particles = strtol(cursor, &endpos, 10);
    cursor += (endpos - cursor);
    //std::cout << "Num particles: " << cur_num_particles;

    if (num_particles != cur_num_particles) {
        //std::cout << std::endl << "Num particles mismatch (" << cur_num_particles << "!= " << num_particles << "). Skipping" << std::endl;
        return 0;
    }

    cursor = skip_token(cursor, " ");  // skip second col
    double weight = strtod(cursor, &endpos);
    cursor += (endpos - cursor);
    //std::cout << " Weight: " << weight << std::endl;

    cursor = nextline(cursor);  // line with first particle

    res->reserve(num_particles*PARTICLE_NUM_VALUES);
    for (int p = 0; p < num_particles; ++p) {
        for (int i = 0; i < PARTICLE_NUM_VALUES-1; i++) {
            cursor = skip_seps(cursor, " ");  // skip second col
            double quantity = strtod(cursor, &endpos);
            cursor += (endpos - cursor);
            res->push_back(quantity);
        }
        res->push_back(weight);
        cursor = nextline(cursor);
    }
    return 1;
}

std::tuple<std::string, int> process_input(const int num_particles, std::string &pre_buf, const char* cursor, std::vector<double>* res) {
    std::string post_buf;
    int num_events = 0;

    if (pre_buf.length() > 0) {
        const char* evt_end = strstr(cursor, "</event>");
        evt_end = strstr(evt_end, "\n") + 1;
        std::string pre_event (cursor, (evt_end-cursor)/(sizeof(char)));
        pre_event = pre_buf + pre_event;
        num_events += process_event(num_particles, pre_event.c_str(), res);
        cursor = evt_end;
    }

    while (cursor && *cursor != '\0') {
        const char* evt_end = strstr(cursor, "</event>");
        if (!evt_end) {
            post_buf = std::string(cursor);
            break;
        }
        num_events += process_event(num_particles, cursor, res);
        cursor = strstr(evt_end, "<event");
        //std::cout << num_events << std::endl;
    }
    return {post_buf, num_events};
}


std::tuple<py::array, py::str> parse_batch(const int num_particles, const char* input_buf, const char* input_batch) {
    std::string output_buf;
    int num_events_inc;
    int num_events = 0;

    std::vector<double>* res = new std::vector<double>();

    std::string init_prebuf("");
    const char* cursor = strstr(input_buf, "<event");
    std::tie(output_buf, num_events_inc) = process_input(num_particles, init_prebuf, cursor, res);
    num_events += num_events_inc;
    std::tie(output_buf, num_events_inc) = process_input(num_particles, output_buf, input_batch, res);
    num_events += num_events_inc;

    auto capsule = py::capsule(res, [] (void *v) {delete reinterpret_cast<std::vector<double>*>(v);});
    auto res_arr =  py::array({num_events, num_particles, PARTICLE_NUM_VALUES},
                              res->data(), capsule);
    auto res_str = py::str(output_buf);
    return {res_arr, res_str};
}

PYBIND11_MODULE(fastlhe, m) {
    m.doc() = "fast lhe format utils"; // optional module docstring

    m.def("parse_batch", &parse_batch, "Parse extract from lhe into numpy array");
}
